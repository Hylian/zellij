use crate::keyboard_parser::{KittyKeyboardParser, KittyParseOutcome};
use crate::os_input_output::ClientOsApi;
#[cfg(windows)]
use crate::os_input_output_windows::use_vt_path;
use crate::stdin_ansi_parser::{HostReply, PendingPartial, StdinAnsiParser};
#[cfg(windows)]
use crate::stdin_handler_windows::enable_vt_input;
use crate::InputInstruction;
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

const LONE_ESC_FLUSH_INTERVAL: Duration = Duration::from_millis(50);
const PARTIAL_REPLY_FLUSH_GUARD: Duration = Duration::from_millis(1000);
use zellij_utils::{
    channels::SenderWithContext,
    data::KeyWithModifier,
    vendored::termwiz::input::{InputEvent, InputParser},
};

pub(crate) fn stdin_loop(
    mut os_input: Box<dyn ClientOsApi>,
    send_input_instructions: SenderWithContext<InputInstruction>,
    stdin_ansi_parser: Arc<Mutex<StdinAnsiParser>>,
    explicitly_disable_kitty_keyboard_protocol: bool,
    support_kitty_graphics_protocol: bool,
    resize_sender: Option<std::sync::mpsc::Sender<()>>,
) {
    // On Windows we choose between the VT byte path (termwiz/kitty parsing)
    // and the native-console path (crossterm INPUT_RECORDs) early, before the
    // startup ANSI query below. See `use_vt_path()` for the trigger conditions.
    #[cfg(windows)]
    let use_vt_reader = use_vt_path() && enable_vt_input();

    // Send the startup host query string so the host terminal replies
    // with its live pixel dimensions, fg/bg, sync-output support, and
    // palette registers. These replies will be classified by the
    // continuous parser as they arrive and routed via `InputInstruction::
    // AnsiStdinInstructions` — no deadline, no cache, no loading gate.
    {
        // On Windows native console, the crossterm event::read() loop
        // reads INPUT_RECORDs via ReadConsoleInput — not raw bytes — so
        // ANSI query responses can never be read on that path.
        #[cfg(windows)]
        let can_query_terminal = use_vt_reader;
        #[cfg(not(windows))]
        let can_query_terminal = true;

        if can_query_terminal {
            let query_string = build_startup_query_string(support_kitty_graphics_protocol);
            let _ = os_input
                .get_stdout_writer()
                .write(query_string.as_bytes())
                .unwrap();
            if support_kitty_graphics_protocol {
                stdin_ansi_parser.lock().unwrap().expect_kitty_probe_reply();
            } else {
                let _ =
                    send_input_instructions.send(InputInstruction::AnsiStdinInstructions(vec![
                        HostReply::KittyGraphicsSupport(false),
                    ]));
            }
        } else {
            let _ = send_input_instructions.send(InputInstruction::AnsiStdinInstructions(vec![
                HostReply::KittyGraphicsSupport(false),
                HostReply::SixelSupport(false),
            ]));
        }
    }

    #[cfg(windows)]
    if !use_vt_reader {
        crate::stdin_handler_windows::native_console_stdin_loop(
            send_input_instructions,
            resize_sender,
        );
        return;
    }

    // Drop the resize sender so the signal handler thread falls back to
    // polling. Only the Windows native console path (above) keeps it alive;
    // the VT reader path and Unix don't produce crossterm resize events.
    drop(resize_sender);

    // Byte reader + unified streaming Kitty/termwiz key decoder path.
    // Used on Unix always, and on Windows inside terminal emulators (Alacritty,
    // etc.) with ENABLE_VIRTUAL_TERMINAL_INPUT enabled so stdin delivers raw VT
    // byte sequences.
    let mut key_decoder = StdinKeyDecoder::new(explicitly_disable_kitty_keyboard_protocol);
    let (stdin_tx, stdin_rx) = mpsc::sync_channel(32);
    let _stdin_pump = std::thread::Builder::new()
        .name("stdin_pump".to_string())
        .spawn({
            move || loop {
                match os_input.read_from_stdin() {
                    Ok(buf) => {
                        if stdin_tx.send(Ok(buf)).is_err() {
                            break; // receiver dropped
                        }
                    },
                    Err(e) => {
                        let _ = stdin_tx.send(Err(e));
                        break;
                    },
                }
            }
        });
    let mut needs_finalization = false;
    let mut reply_in_progress_since: Option<Instant> = None;
    let mut key_in_progress_since: Option<Instant> = None;
    'stdin: loop {
        match if needs_finalization {
            stdin_rx.recv_timeout(LONE_ESC_FLUSH_INTERVAL)
        } else {
            stdin_rx
                .recv()
                .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
        } {
            Ok(result) => {
                match result {
                    Ok(buf) => {
                        // Strip + classify any host-reply sequences
                        // continuously. The residue is the byte stream
                        // the keyboard parser should see.
                        let parse_output = {
                            let mut p = stdin_ansi_parser.lock().unwrap();
                            p.feed(&buf)
                        };
                        if !parse_output.replies.is_empty() {
                            let _ = send_input_instructions.send(
                                InputInstruction::AnsiStdinInstructions(parse_output.replies),
                            );
                        }
                        if let Some((token, reply_bytes)) = parse_output.completed_forward {
                            let _ = send_input_instructions.send(
                                InputInstruction::ForwardedReplyFromHostComplete {
                                    token,
                                    reply_bytes,
                                },
                            );
                        }
                        if let Some((token, reply_bytes)) = parse_output.completed_clipboard_forward
                        {
                            let _ = send_input_instructions.send(
                                InputInstruction::ForwardedReplyFromHostComplete {
                                    token,
                                    reply_bytes,
                                },
                            );
                        }
                        for payload in parse_output.desktop_notifications {
                            let _ = send_input_instructions
                                .send(InputInstruction::DesktopNotificationResponse(payload));
                        }
                        for payload_bytes in parse_output.nested_frames {
                            let _ = send_input_instructions
                                .send(InputInstruction::NestedSessionFrameFromHost(payload_bytes));
                        }
                        let (residue, focus_changes) = extract_focus_reports(parse_output.residue);
                        for focused in focus_changes {
                            let _ = send_input_instructions
                                .send(InputInstruction::HostTerminalFocusChanged(focused));
                        }
                        if !residue.is_empty() {
                            let events = key_decoder.feed(&residue);
                            if !dispatch_decoded_events(events, &send_input_instructions) {
                                break 'stdin;
                            }
                            if key_decoder.pending_state() == PendingInputState::InFlightSequence {
                                key_in_progress_since = Some(Instant::now());
                            } else {
                                key_in_progress_since = None;
                            }
                        }
                        update_finalization_state(
                            &stdin_ansi_parser,
                            &key_decoder,
                            &mut needs_finalization,
                            &mut reply_in_progress_since,
                            &mut key_in_progress_since,
                        );
                    },
                    Err(e) => {
                        if e == "Session ended" {
                            log::debug!("Switched sessions, signing this thread off...");
                        } else {
                            log::error!("Failed to read from STDIN: {}", e);
                        }
                        let _ = send_input_instructions.send(InputInstruction::Exit);
                        break;
                    },
                }
            },
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let ansi_pending = stdin_ansi_parser.lock().unwrap().pending_partial();
                if ansi_pending != PendingPartial::None {
                    if ansi_pending.uses_reply_flush_guard() {
                        let elapsed = reply_in_progress_since
                            .map(|since| since.elapsed())
                            .unwrap_or_default();
                        if elapsed >= PARTIAL_REPLY_FLUSH_GUARD {
                            let drained = stdin_ansi_parser.lock().unwrap().finalize_force();
                            if !drained.is_empty() {
                                let mut events = key_decoder.feed(&drained);
                                events.extend(key_decoder.flush_force());
                                if !dispatch_decoded_events(events, &send_input_instructions) {
                                    break 'stdin;
                                }
                            }
                            reply_in_progress_since = None;
                        }
                    } else {
                        let drained = stdin_ansi_parser.lock().unwrap().finalize_fast_partial();
                        if !drained.is_empty() {
                            let mut events = key_decoder.feed(&drained);
                            events.extend(key_decoder.flush_fast_timeout());
                            if !dispatch_decoded_events(events, &send_input_instructions) {
                                break 'stdin;
                            }
                        }
                        reply_in_progress_since = None;
                    }
                }

                match key_decoder.pending_state() {
                    PendingInputState::None => {
                        key_in_progress_since = None;
                    },
                    PendingInputState::FastTimeout => {
                        let events = key_decoder.flush_fast_timeout();
                        if !dispatch_decoded_events(events, &send_input_instructions) {
                            break 'stdin;
                        }
                        key_in_progress_since = None;
                    },
                    PendingInputState::InFlightSequence => {
                        let elapsed = key_in_progress_since
                            .map(|since| since.elapsed())
                            .unwrap_or_default();
                        if elapsed >= PARTIAL_REPLY_FLUSH_GUARD {
                            let events = key_decoder.flush_force();
                            if !dispatch_decoded_events(events, &send_input_instructions) {
                                break 'stdin;
                            }
                            key_in_progress_since = None;
                        }
                    },
                }

                update_finalization_state(
                    &stdin_ansi_parser,
                    &key_decoder,
                    &mut needs_finalization,
                    &mut reply_in_progress_since,
                    &mut key_in_progress_since,
                );
            },
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                log::debug!("STDIN pump disconnected");
                let _ = send_input_instructions.send(InputInstruction::Exit);
                break;
            },
        }
    }
}

fn dispatch_decoded_events(
    events: Vec<DecodedStdinEvent>,
    send_input_instructions: &SenderWithContext<InputInstruction>,
) -> bool {
    for event in events {
        let instruction = match event {
            DecodedStdinEvent::KittyKey(key, raw_bytes) => {
                InputInstruction::KeyWithModifierEvent(key, raw_bytes, true)
            },
            DecodedStdinEvent::Termwiz(input_event, raw_bytes) => {
                InputInstruction::KeyEvent(input_event, raw_bytes)
            },
        };
        if send_input_instructions.send(instruction).is_err() {
            return false;
        }
    }
    true
}

fn update_finalization_state(
    stdin_ansi_parser: &Arc<Mutex<StdinAnsiParser>>,
    key_decoder: &StdinKeyDecoder,
    needs_finalization: &mut bool,
    reply_in_progress_since: &mut Option<Instant>,
    key_in_progress_since: &mut Option<Instant>,
) {
    let ansi_pending = stdin_ansi_parser.lock().unwrap().pending_partial();
    let key_pending = key_decoder.pending_state();

    *needs_finalization =
        ansi_pending != PendingPartial::None || key_pending != PendingInputState::None;

    if ansi_pending.uses_reply_flush_guard() {
        if reply_in_progress_since.is_none() {
            *reply_in_progress_since = Some(Instant::now());
        }
    } else {
        *reply_in_progress_since = None;
    }

    if key_pending == PendingInputState::InFlightSequence {
        if key_in_progress_since.is_none() {
            *key_in_progress_since = Some(Instant::now());
        }
    } else {
        *key_in_progress_since = None;
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DecodedStdinEvent {
    KittyKey(KeyWithModifier, Vec<u8>),
    Termwiz(InputEvent, Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PendingInputState {
    None,
    /// Ambiguous short escape prefix (`\x1b`, `\x1b[`, `\x1bO`, or incomplete UTF-8 char)
    /// that flushes after `LONE_ESC_FLUSH_INTERVAL` (50ms) when idle.
    FastTimeout,
    /// In-flight multi-byte CSI/SS3 sequence (`\x1b[<params>...`, `\x1bO<params>...`)
    /// split across reads; held up to `PARTIAL_REPLY_FLUSH_GUARD` (1000ms) so SSH jitter
    /// never splits an escape sequence into spurious Esc + literal tail bytes.
    InFlightSequence,
}

const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";
const MAX_INFLIGHT_SEQ_LEN: usize = 64;

/// Unified streaming stdin tokenizer that slices coalesced byte streams into
/// individual Kitty CSI events and termwiz events while maintaining a single
/// pending byte buffer across fragmented reads.
pub(crate) struct StdinKeyDecoder {
    input_parser: InputParser,
    pending: Vec<u8>,
    explicitly_disable_kitty_keyboard_protocol: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeqScan {
    Complete(usize),
    Incomplete,
    Aborted(usize),
}

fn scan_csi(buf: &[u8]) -> SeqScan {
    debug_assert!(buf.starts_with(b"\x1b["));
    if buf.len() == 2 {
        return SeqScan::Incomplete;
    }
    let mut idx = 2;
    while idx < buf.len() && (0x30..=0x3F).contains(&buf[idx]) {
        idx += 1;
    }
    while idx < buf.len() && (0x20..=0x2F).contains(&buf[idx]) {
        idx += 1;
    }
    if idx == buf.len() {
        if buf.len() <= MAX_INFLIGHT_SEQ_LEN {
            SeqScan::Incomplete
        } else {
            SeqScan::Aborted(buf.len())
        }
    } else if (0x40..=0x7E).contains(&buf[idx]) {
        SeqScan::Complete(idx + 1)
    } else {
        SeqScan::Aborted(idx)
    }
}

fn scan_ss3(buf: &[u8]) -> SeqScan {
    debug_assert!(buf.starts_with(b"\x1bO"));
    if buf.len() == 2 {
        return SeqScan::Incomplete;
    }
    if (0x40..=0x7E).contains(&buf[2]) {
        return SeqScan::Complete(3);
    }
    if buf[2] != b'1' {
        return SeqScan::Aborted(2);
    }
    let mut idx = 3;
    while idx < buf.len() && (0x30..=0x3F).contains(&buf[idx]) {
        idx += 1;
    }
    if idx == buf.len() {
        if buf.len() <= MAX_INFLIGHT_SEQ_LEN {
            SeqScan::Incomplete
        } else {
            SeqScan::Aborted(buf.len())
        }
    } else if (0x40..=0x7E).contains(&buf[idx]) {
        SeqScan::Complete(idx + 1)
    } else {
        SeqScan::Aborted(idx)
    }
}

fn utf8_expected_len(first_byte: u8) -> usize {
    match first_byte {
        0xC2..=0xDF => 2,
        0xE0..=0xEF => 3,
        0xF0..=0xF4 => 4,
        _ => 1,
    }
}

fn trailing_incomplete_utf8_len(bytes: &[u8]) -> usize {
    let max_back = bytes.len().min(3);
    for back in 1..=max_back {
        let pos = bytes.len() - back;
        let b = bytes[pos];
        if (b & 0xC0) != 0x80 {
            let expected = utf8_expected_len(b);
            if expected > back && bytes[pos + 1..].iter().all(|&c| (c & 0xC0) == 0x80) {
                return back;
            }
            return 0;
        }
    }
    0
}

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn trailing_prefix_len(buf: &[u8], marker: &[u8]) -> usize {
    let max_k = buf.len().min(marker.len().saturating_sub(1));
    for k in (1..=max_k).rev() {
        if &buf[buf.len() - k..] == &marker[..k] {
            return k;
        }
    }
    0
}

enum KittyCsiMatch {
    Key(KeyWithModifier),
    IgnoredRelease,
    NoMatch,
}

fn csi_is_kitty_key_release(csi_seq: &[u8]) -> bool {
    if csi_seq.len() < 3 || !csi_seq.starts_with(b"\x1b[") {
        return false;
    }
    let body = &csi_seq[2..csi_seq.len() - 1];
    let Some(mod_param) = body.split(|&b| b == b';').nth(1) else {
        return false;
    };
    mod_param.split(|&b| b == b':').nth(1) == Some(b"3")
}

impl StdinKeyDecoder {
    pub(crate) fn new(explicitly_disable_kitty_keyboard_protocol: bool) -> Self {
        Self {
            input_parser: InputParser::new(),
            pending: Vec::new(),
            explicitly_disable_kitty_keyboard_protocol,
        }
    }

    pub(crate) fn pending_state(&self) -> PendingInputState {
        if self.input_parser.is_pasting() {
            // While inside bracketed paste, wait passively for `\x1b[201~`
            // without triggering 50ms/1000ms escape flushes.
            return PendingInputState::None;
        }
        if self.pending.is_empty() {
            return PendingInputState::None;
        }
        if (self.pending.starts_with(b"\x1b[") && self.pending.len() >= 3)
            || self.pending.starts_with(b"\x1bO1")
            || (self.pending.starts_with(b"\x1b\x1b[") && self.pending.len() >= 4)
            || self.pending.starts_with(b"\x1b\x1bO1")
        {
            PendingInputState::InFlightSequence
        } else {
            PendingInputState::FastTimeout
        }
    }

    pub(crate) fn feed(&mut self, bytes: &[u8]) -> Vec<DecodedStdinEvent> {
        if !bytes.is_empty() {
            self.pending.extend_from_slice(bytes);
        }
        self.drain_complete()
    }

    pub(crate) fn flush_fast_timeout(&mut self) -> Vec<DecodedStdinEvent> {
        if self.pending_state() != PendingInputState::FastTimeout {
            return Vec::new();
        }
        let drained = std::mem::take(&mut self.pending);
        let mut out = Vec::new();
        self.feed_termwiz_slice(&drained, false, &mut out);
        out
    }

    pub(crate) fn flush_force(&mut self) -> Vec<DecodedStdinEvent> {
        if self.input_parser.is_pasting() || self.pending.is_empty() {
            return Vec::new();
        }
        let drained = std::mem::take(&mut self.pending);
        let mut out = Vec::new();
        self.feed_termwiz_slice(&drained, false, &mut out);
        out
    }

    fn feed_termwiz_slice(
        &mut self,
        chunk: &[u8],
        maybe_more: bool,
        out: &mut Vec<DecodedStdinEvent>,
    ) {
        let mut cursor = 0usize;
        self.input_parser.parse_with_consumed(
            chunk,
            |input_event, consumed| {
                let end = cursor.saturating_add(consumed).min(chunk.len());
                let raw = chunk[cursor..end].to_vec();
                cursor = end;
                out.push(DecodedStdinEvent::Termwiz(input_event, raw));
            },
            maybe_more,
        );
        if !maybe_more && self.input_parser.buffered_len() > 0 && !self.input_parser.is_pasting() {
            self.input_parser.parse_with_consumed(
                &[],
                |input_event, consumed| {
                    let end = cursor.saturating_add(consumed).min(chunk.len());
                    let raw = chunk[cursor..end].to_vec();
                    cursor = end;
                    out.push(DecodedStdinEvent::Termwiz(input_event, raw));
                },
                false,
            );
        }
        if !self.input_parser.is_pasting() && self.input_parser.buffered_len() > 0 {
            self.input_parser = InputParser::new();
        }
    }

    fn try_parse_kitty(&self, csi_seq: &[u8]) -> KittyCsiMatch {
        if self.explicitly_disable_kitty_keyboard_protocol {
            return KittyCsiMatch::NoMatch;
        }
        match KittyKeyboardParser::new().feed(csi_seq) {
            KittyParseOutcome::Complete(key) => {
                if csi_is_kitty_key_release(csi_seq) {
                    KittyCsiMatch::IgnoredRelease
                } else {
                    KittyCsiMatch::Key(key)
                }
            },
            KittyParseOutcome::Incomplete | KittyParseOutcome::NoMatch => KittyCsiMatch::NoMatch,
        }
    }

    fn drain_complete(&mut self) -> Vec<DecodedStdinEvent> {
        let mut out = Vec::new();
        while !self.pending.is_empty() {
            if self.input_parser.is_pasting() {
                if let Some(pos) = find_subsequence(&self.pending, PASTE_END) {
                    let chunk: Vec<u8> = self.pending.drain(..pos + PASTE_END.len()).collect();
                    self.feed_termwiz_slice(&chunk, true, &mut out);
                    continue;
                } else {
                    let keep = trailing_prefix_len(&self.pending, PASTE_END);
                    let safe_len = self.pending.len() - keep;
                    if safe_len > 0 {
                        let chunk: Vec<u8> = self.pending.drain(..safe_len).collect();
                        self.feed_termwiz_slice(&chunk, true, &mut out);
                    }
                    break;
                }
            }

            if self.pending[0] != 0x1b {
                if let Some(next_esc) = self.pending.iter().position(|&b| b == 0x1b) {
                    let chunk: Vec<u8> = self.pending.drain(..next_esc).collect();
                    self.feed_termwiz_slice(&chunk, false, &mut out);
                    continue;
                } else {
                    let incomplete_tail = trailing_incomplete_utf8_len(&self.pending);
                    let complete_len = self.pending.len() - incomplete_tail;
                    if complete_len > 0 {
                        let chunk: Vec<u8> = self.pending.drain(..complete_len).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    }
                    if incomplete_tail > 0 {
                        break;
                    }
                    continue;
                }
            }

            // Starts with `\x1b`
            if self.pending.len() == 1 {
                break;
            }

            match self.pending[1] {
                b'[' => match scan_csi(&self.pending) {
                    SeqScan::Incomplete => break,
                    SeqScan::Aborted(len) => {
                        let chunk: Vec<u8> = self.pending.drain(..len).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    },
                    SeqScan::Complete(csi_len) => {
                        if &self.pending[..csi_len] == PASTE_START {
                            let chunk: Vec<u8> = self.pending.drain(..csi_len).collect();
                            self.feed_termwiz_slice(&chunk, true, &mut out);
                        } else {
                            match self.try_parse_kitty(&self.pending[..csi_len]) {
                                KittyCsiMatch::Key(key) => {
                                    let raw: Vec<u8> = self.pending.drain(..csi_len).collect();
                                    out.push(DecodedStdinEvent::KittyKey(key, raw));
                                },
                                KittyCsiMatch::IgnoredRelease => {
                                    self.pending.drain(..csi_len);
                                },
                                KittyCsiMatch::NoMatch => {
                                    let chunk: Vec<u8> = self.pending.drain(..csi_len).collect();
                                    self.feed_termwiz_slice(&chunk, true, &mut out);
                                },
                            }
                        }
                    },
                },
                b'O' => match scan_ss3(&self.pending) {
                    SeqScan::Incomplete => break,
                    SeqScan::Aborted(len) => {
                        let chunk: Vec<u8> = self.pending.drain(..len).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    },
                    SeqScan::Complete(ss3_len) => {
                        let chunk: Vec<u8> = self.pending.drain(..ss3_len).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    },
                },
                0x1b => {
                    if self.pending.len() == 2 {
                        break;
                    }
                    match self.pending[2] {
                        b'[' => match scan_csi(&self.pending[1..]) {
                            SeqScan::Incomplete => break,
                            SeqScan::Aborted(_) => {
                                let esc: Vec<u8> = self.pending.drain(..1).collect();
                                self.feed_termwiz_slice(&esc, false, &mut out);
                            },
                            SeqScan::Complete(csi_len) => {
                                let inner = &self.pending[1..1 + csi_len];
                                let is_kitty_only_csi = (inner.ends_with(b"u")
                                    || inner.contains(&b':'))
                                    && !matches!(
                                        self.try_parse_kitty(inner),
                                        KittyCsiMatch::NoMatch
                                    );
                                if inner == PASTE_START || is_kitty_only_csi {
                                    // Unambiguous Kitty CSI sequences and bracketed paste never use
                                    // a leading ESC prefix for Alt — flush the standalone ESC first.
                                    let esc: Vec<u8> = self.pending.drain(..1).collect();
                                    self.feed_termwiz_slice(&esc, false, &mut out);
                                } else {
                                    // Legacy `\x1b\x1b[A` (Alt+Arrow), `\x1b\x1b[5~` (Alt+PageUp),
                                    // or `\x1b\x1b[<...M` (Esc + Mouse).
                                    let chunk: Vec<u8> =
                                        self.pending.drain(..1 + csi_len).collect();
                                    self.feed_termwiz_slice(&chunk, true, &mut out);
                                }
                            },
                        },
                        b'O' => match scan_ss3(&self.pending[1..]) {
                            SeqScan::Incomplete => break,
                            SeqScan::Aborted(_) => {
                                let esc: Vec<u8> = self.pending.drain(..1).collect();
                                self.feed_termwiz_slice(&esc, false, &mut out);
                            },
                            SeqScan::Complete(ss3_len) => {
                                let chunk: Vec<u8> = self.pending.drain(..1 + ss3_len).collect();
                                self.feed_termwiz_slice(&chunk, false, &mut out);
                            },
                        },
                        _ => {
                            let esc: Vec<u8> = self.pending.drain(..1).collect();
                            self.feed_termwiz_slice(&esc, false, &mut out);
                        },
                    }
                },
                b1 => {
                    let expected = utf8_expected_len(b1);
                    if self.pending.len() < 1 + expected {
                        if self.pending[2..].iter().all(|&c| (c & 0xC0) == 0x80) {
                            break;
                        }
                        let chunk: Vec<u8> = self.pending.drain(..2).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    } else {
                        let take = if std::str::from_utf8(&self.pending[1..1 + expected]).is_ok() {
                            1 + expected
                        } else {
                            2
                        };
                        let chunk: Vec<u8> = self.pending.drain(..take).collect();
                        self.feed_termwiz_slice(&chunk, false, &mut out);
                    }
                },
            }
        }
        out
    }
}

fn extract_focus_reports(residue: Vec<u8>) -> (Vec<u8>, Vec<bool>) {
    const FOCUS_GAINED: &[u8] = b"\x1b[I";
    const FOCUS_LOST: &[u8] = b"\x1b[O";
    if residue.len() < FOCUS_GAINED.len() {
        return (residue, vec![]);
    }
    let mut remaining = Vec::with_capacity(residue.len());
    let mut focus_changes = vec![];
    let mut index = 0;
    while index < residue.len() {
        let rest = &residue[index..];
        if rest.starts_with(FOCUS_GAINED) {
            focus_changes.push(true);
            index += FOCUS_GAINED.len();
        } else if rest.starts_with(FOCUS_LOST) {
            focus_changes.push(false);
            index += FOCUS_LOST.len();
        } else {
            remaining.push(residue[index]);
            index += 1;
        }
    }
    (remaining, focus_changes)
}

/// Trim `current_buffer` to the parser's own buffered length so it can
/// never drift from the parser's internal state: a trailing incomplete
/// sequence is held by the parser (and mirrored here) until the next
/// read completes it, while bytes already decoded into events are dropped.
#[cfg(test)]
fn realign_current_buffer(current_buffer: &mut Vec<u8>, input_parser: &InputParser) {
    let buffered = input_parser.buffered_len();
    let excess = current_buffer.len().saturating_sub(buffered);
    if excess > 0 {
        current_buffer.drain(..excess);
    }
}

/// Build the fire-and-forget host-query batch sent at client startup.
/// The host's replies refine `Screen`'s cached state asynchronously as
/// they arrive; the UI does not block on them.
fn build_startup_query_string(support_kitty_graphics_protocol: bool) -> String {
    // <ESC>[14t => get text area size in pixels,
    // <ESC>[16t => get character cell size in pixels
    // <ESC>]11;?<ESC>\ => get background color
    // <ESC>]10;?<ESC>\ => get foreground color
    // <ESC>[?2026$p => get synchronised output mode
    // <ESC>_Ga=q,...<ESC>\ => probe kitty graphics support (omitted when the
    // protocol is disabled), answered by capable terminals only; the trailing
    // Primary DA is the barrier that resolves the probe negatively when it
    // goes unanswered
    let kitty_graphics_probe = if support_kitty_graphics_protocol {
        "\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}"
    } else {
        ""
    };
    format!(
        "{}\u{1b}]11;?\u{1b}\u{5c}\u{1b}]10;?\u{1b}\u{5c}\u{1b}[?2026$p{}\u{1b}[c",
        PIXEL_SIZE_QUERY, kitty_graphics_probe
    )
}

pub(crate) const PIXEL_SIZE_QUERY: &str = "\u{1b}[14t\u{1b}[16t";

#[cfg(test)]
mod tests {
    use super::{
        build_startup_query_string, extract_focus_reports, realign_current_buffer, InputParser,
        PIXEL_SIZE_QUERY,
    };

    #[test]
    fn realign_after_lone_paste_start_empties_the_buffer() {
        let mut parser = InputParser::new();
        parser.parse_with_consumed(b"\x1b[200~", |_, _| {}, true);
        let mut current_buffer = b"\x1b[200~".to_vec();
        realign_current_buffer(&mut current_buffer, &parser);
        assert!(
            current_buffer.is_empty(),
            "the silently consumed paste-start bytes must not linger: {:?}",
            current_buffer
        );
    }

    #[test]
    fn realign_drops_stale_bytes_from_the_front_and_keeps_the_pending_tail() {
        let mut parser = InputParser::new();
        parser.parse_with_consumed(b"\x1b[200~hel", |_, _| {}, true);
        let mut current_buffer = b"\x1b[200~hel".to_vec();
        realign_current_buffer(&mut current_buffer, &parser);
        assert_eq!(
            current_buffer, b"hel",
            "the retained bytes must be the parser's pending tail, not the stale front"
        );
    }

    #[test]
    fn realign_is_a_no_op_when_nothing_was_consumed() {
        let mut parser = InputParser::new();
        parser.parse_with_consumed(b"\x1b[1;2", |_, _| {}, true);
        let mut current_buffer = b"\x1b[1;2".to_vec();
        realign_current_buffer(&mut current_buffer, &parser);
        assert_eq!(current_buffer, b"\x1b[1;2");
    }

    #[test]
    fn realign_tolerates_a_shorter_mirror_buffer() {
        let mut parser = InputParser::new();
        parser.parse_with_consumed(b"\x1b[1;2", |_, _| {}, true);
        let mut current_buffer = b";2".to_vec();
        realign_current_buffer(&mut current_buffer, &parser);
        assert_eq!(current_buffer, b";2");
    }

    #[test]
    fn pixel_size_query_probes_text_area_and_character_cell() {
        assert_eq!(PIXEL_SIZE_QUERY, "\u{1b}[14t\u{1b}[16t");
    }

    #[test]
    fn startup_query_has_no_palette_register_loop() {
        let query = build_startup_query_string(true);
        assert_eq!(
            query,
            "\u{1b}[14t\u{1b}[16t\u{1b}]11;?\u{1b}\u{5c}\u{1b}]10;?\u{1b}\u{5c}\u{1b}[?2026$p\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}\u{1b}[c"
        );
        assert!(
            !query.contains("\u{1b}]4;"),
            "startup query must not contain OSC 4 palette-register probes: {:?}",
            query
        );
    }

    #[test]
    fn focus_reports_are_extracted_from_the_byte_stream() {
        let (residue, focus_changes) = extract_focus_reports(b"a\x1b[Ib\x1b[Oc".to_vec());
        assert_eq!(residue, b"abc".to_vec());
        assert_eq!(focus_changes, vec![true, false]);
    }

    #[test]
    fn a_stream_without_focus_reports_is_left_untouched() {
        let (residue, focus_changes) = extract_focus_reports(b"\x1b[A\x1b[B".to_vec());
        assert_eq!(residue, b"\x1b[A\x1b[B".to_vec());
        assert!(focus_changes.is_empty());
    }

    #[test]
    fn startup_query_contains_kitty_probe_before_barrier() {
        let query = build_startup_query_string(true);
        assert!(query.contains("\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}"));
        let probe_pos = query.find("\u{1b}_Ga=q,i=31,").unwrap();
        let barrier_pos = query.find("\u{1b}[c").unwrap();
        assert!(probe_pos < barrier_pos);
    }

    #[test]
    fn startup_query_omits_kitty_probe_when_the_protocol_is_disabled() {
        let query = build_startup_query_string(false);
        assert!(!query.contains("\u{1b}_G"));
        assert!(query.ends_with("\u{1b}[c"));
        assert!(query.starts_with("\u{1b}[14t\u{1b}[16t"));
    }

    #[test]
    fn coalesced_kitty_repeats_in_single_read_emit_every_event_as_kitty() {
        use super::{DecodedStdinEvent, PendingInputState, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};

        let mut dec = StdinKeyDecoder::new(false);
        let events = dec.feed(b"\x1b[107;5u\x1b[107;5u\x1b[107;5u");
        let expected = DecodedStdinEvent::KittyKey(
            KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
            b"\x1b[107;5u".to_vec(),
        );
        assert_eq!(events, vec![expected.clone(), expected.clone(), expected]);
        assert_eq!(dec.pending_state(), PendingInputState::None);
    }

    #[test]
    fn split_kitty_sequence_over_ssh_jitter_plus_coalesced_repeat_never_leaks_garbage() {
        use super::{DecodedStdinEvent, PendingInputState, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};

        let mut dec = StdinKeyDecoder::new(false);

        // First TCP segment ends mid-CSI (`\x1b[1`).
        assert!(dec.feed(b"\x1b[1").is_empty());
        assert_eq!(dec.pending_state(), PendingInputState::InFlightSequence);

        // 50ms lone-ESC timer fires during an SSH latency spike — must NOT flush
        // an in-flight CSI sequence into spurious Alt+[ + '1'.
        assert!(dec.flush_fast_timeout().is_empty());

        // Second TCP segment brings the remainder `07;5u` coalesced with the next repeat.
        let events = dec.feed(b"07;5u\x1b[107;5u");
        let expected = DecodedStdinEvent::KittyKey(
            KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
            b"\x1b[107;5u".to_vec(),
        );
        assert_eq!(events, vec![expected.clone(), expected]);
        assert_eq!(dec.pending_state(), PendingInputState::None);
    }

    #[test]
    fn kitty_subparameters_for_repeat_and_shifted_keys_decode_cleanly() {
        use super::{DecodedStdinEvent, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};

        let mut dec = StdinKeyDecoder::new(false);
        let events = dec.feed(b"\x1b[107;5:2u\x1b[91:123;4u");
        assert_eq!(
            events,
            vec![
                DecodedStdinEvent::KittyKey(
                    KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
                    b"\x1b[107;5:2u".to_vec(),
                ),
                DecodedStdinEvent::KittyKey(
                    KeyWithModifier::new(BareKey::Char('['))
                        .with_alt_modifier()
                        .with_shift_modifier(),
                    b"\x1b[91:123;4u".to_vec(),
                ),
            ]
        );
    }

    #[test]
    fn mixed_text_kitty_brackets_and_sgr_mouse_in_one_chunk() {
        use super::{DecodedStdinEvent, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};
        use zellij_utils::vendored::termwiz::input::{InputEvent, KeyCode, KeyEvent, Modifiers};

        let mut dec = StdinKeyDecoder::new(false);
        let events = dec.feed(b"a\x1b[91;3u\x1b[<35;10;20M\x1b[93;3u");
        assert_eq!(events.len(), 4);
        assert_eq!(
            events[0],
            DecodedStdinEvent::Termwiz(
                InputEvent::Key(KeyEvent {
                    key: KeyCode::Char('a'),
                    modifiers: Modifiers::NONE,
                }),
                b"a".to_vec(),
            )
        );
        assert_eq!(
            events[1],
            DecodedStdinEvent::KittyKey(
                KeyWithModifier::new(BareKey::Char('[')).with_alt_modifier(),
                b"\x1b[91;3u".to_vec(),
            )
        );
        assert!(matches!(
            events[2],
            DecodedStdinEvent::Termwiz(InputEvent::Mouse(_), _)
        ));
        assert_eq!(
            events[3],
            DecodedStdinEvent::KittyKey(
                KeyWithModifier::new(BareKey::Char(']')).with_alt_modifier(),
                b"\x1b[93;3u".to_vec(),
            )
        );
    }

    #[test]
    fn bracketed_paste_containing_kitty_sequences_and_split_end_marker() {
        use super::{DecodedStdinEvent, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};
        use zellij_utils::vendored::termwiz::input::InputEvent;

        let mut dec = StdinKeyDecoder::new(false);
        assert!(dec.feed(b"\x1b[200~literal \x1b[107;5u\x1b[20").is_empty());
        let events = dec.feed(b"1~\x1b[91;3u");
        assert_eq!(events.len(), 2);
        assert!(matches!(
            &events[0],
            DecodedStdinEvent::Termwiz(InputEvent::Paste(s), _) if s == "literal \x1b[107;5u"
        ));
        assert_eq!(
            events[1],
            DecodedStdinEvent::KittyKey(
                KeyWithModifier::new(BareKey::Char('[')).with_alt_modifier(),
                b"\x1b[91;3u".to_vec(),
            )
        );
    }

    #[test]
    fn lone_esc_flushes_on_fast_timeout() {
        use super::{DecodedStdinEvent, PendingInputState, StdinKeyDecoder};
        use zellij_utils::vendored::termwiz::input::{InputEvent, KeyCode, KeyEvent, Modifiers};

        let mut dec = StdinKeyDecoder::new(false);
        assert!(dec.feed(b"\x1b").is_empty());
        assert_eq!(dec.pending_state(), PendingInputState::FastTimeout);
        let events = dec.flush_fast_timeout();
        assert_eq!(
            events,
            vec![DecodedStdinEvent::Termwiz(
                InputEvent::Key(KeyEvent {
                    key: KeyCode::Escape,
                    modifiers: Modifiers::NONE,
                }),
                b"\x1b".to_vec(),
            )]
        );
        assert_eq!(dec.pending_state(), PendingInputState::None);
    }

    #[test]
    fn kitty_event_type_subparams_keep_press_and_repeat_but_drop_release() {
        use super::{DecodedStdinEvent, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};

        let mut dec = StdinKeyDecoder::new(false);
        // press (:1 with associated text ;107), repeat (:2), release (:3), bare release (1:3)
        let events = dec.feed(b"\x1b[107;5:1;107u\x1b[107;5:2u\x1b[107;5:3u\x1b[97;1:3u");
        assert_eq!(
            events,
            vec![
                DecodedStdinEvent::KittyKey(
                    KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
                    b"\x1b[107;5:1;107u".to_vec(),
                ),
                DecodedStdinEvent::KittyKey(
                    KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
                    b"\x1b[107;5:2u".to_vec(),
                ),
            ]
        );
    }

    #[test]
    fn ansi_scrubber_plus_key_decoder_under_every_split_offset() {
        use super::{DecodedStdinEvent, StdinAnsiParser, StdinKeyDecoder};
        use zellij_utils::data::{BareKey, KeyWithModifier};

        let stream = b"\x1b[107;5u".repeat(12);
        for chunk_size in 1..=17 {
            let mut ansi = StdinAnsiParser::new();
            let mut dec = StdinKeyDecoder::new(false);
            let mut all_events = Vec::new();
            for chunk in stream.chunks(chunk_size) {
                let parsed = ansi.feed(chunk);
                if !parsed.residue.is_empty() {
                    all_events.extend(dec.feed(&parsed.residue));
                }
            }
            assert_eq!(
                all_events.len(),
                12,
                "chunk_size={} dropped or duplicated events",
                chunk_size
            );
            for ev in all_events {
                assert_eq!(
                    ev,
                    DecodedStdinEvent::KittyKey(
                        KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
                        b"\x1b[107;5u".to_vec(),
                    )
                );
            }
        }
    }

    #[test]
    fn legacy_esc_prefixed_arrow_and_page_up_preserve_alt_modifier() {
        use super::{DecodedStdinEvent, StdinKeyDecoder};
        use zellij_utils::vendored::termwiz::input::{InputEvent, KeyCode, KeyEvent, Modifiers};

        let mut dec = StdinKeyDecoder::new(false);
        let events = dec.feed(b"\x1b\x1b[A\x1b\x1b[5~");
        assert_eq!(
            events,
            vec![
                DecodedStdinEvent::Termwiz(
                    InputEvent::Key(KeyEvent {
                        key: KeyCode::UpArrow,
                        modifiers: Modifiers::ALT,
                    }),
                    b"\x1b\x1b[A".to_vec(),
                ),
                DecodedStdinEvent::Termwiz(
                    InputEvent::Key(KeyEvent {
                        key: KeyCode::PageUp,
                        modifiers: Modifiers::ALT,
                    }),
                    b"\x1b\x1b[5~".to_vec(),
                ),
            ]
        );
    }

    #[test]
    fn unrecognized_private_and_angle_csi_never_wedge_following_keys() {
        use super::{DecodedStdinEvent, PendingInputState, StdinKeyDecoder};
        use zellij_utils::vendored::termwiz::input::{InputEvent, KeyCode, KeyEvent, Modifiers};

        let mut dec = StdinKeyDecoder::new(false);
        assert!(dec.feed(b"\x1b[?1u\x1b[<u").is_empty());
        assert_eq!(dec.pending_state(), PendingInputState::None);

        let events = dec.feed(b"a");
        assert_eq!(
            events,
            vec![DecodedStdinEvent::Termwiz(
                InputEvent::Key(KeyEvent {
                    key: KeyCode::Char('a'),
                    modifiers: Modifiers::NONE,
                }),
                b"a".to_vec(),
            )]
        );
    }
}
