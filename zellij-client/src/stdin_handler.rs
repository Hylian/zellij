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
    query_host_theme: bool,
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
            let query_string =
                build_startup_query_string(support_kitty_graphics_protocol, query_host_theme);
            {
                let mut stdin_ansi_parser = stdin_ansi_parser.lock().unwrap();
                stdin_ansi_parser.open_own_query_batch();
                stdin_ansi_parser.expect_grapheme_cluster_mode_reply();
                if support_kitty_graphics_protocol {
                    stdin_ansi_parser.expect_kitty_probe_reply();
                    stdin_ansi_parser.expect_kitty_zlib_probe_reply();
                }
                let mut stdout = os_input.get_stdout_writer();
                let _ = stdout.write_all(query_string.as_bytes());
                let _ = stdout.flush();
            }
            if !support_kitty_graphics_protocol {
                let _ =
                    send_input_instructions.send(InputInstruction::AnsiStdinInstructions(vec![
                        HostReply::KittyGraphicsSupport(false),
                        HostReply::KittyZlibSupport(false),
                    ]));
            }
        } else {
            let _ = send_input_instructions.send(InputInstruction::AnsiStdinInstructions(vec![
                HostReply::KittyGraphicsSupport(false),
                HostReply::KittyZlibSupport(false),
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
                        let (parse_output, exit_draining) = {
                            let mut p = stdin_ansi_parser.lock().unwrap();
                            let parse_output = p.feed(&buf);
                            (parse_output, p.is_exit_draining())
                        };
                        if exit_draining {
                            key_decoder.clear();
                            needs_finalization = false;
                            reply_in_progress_since = None;
                            continue;
                        }
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
                        }
                        update_finalization_state(
                            &stdin_ansi_parser,
                            &key_decoder,
                            &mut needs_finalization,
                            &mut reply_in_progress_since,
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
                if stdin_ansi_parser.lock().unwrap().is_exit_draining() {
                    key_decoder.clear();
                    needs_finalization = false;
                    reply_in_progress_since = None;
                    continue;
                }
                let guarded = stdin_ansi_parser
                    .lock()
                    .unwrap()
                    .pending_partial()
                    .uses_reply_flush_guard();
                let elapsed = reply_in_progress_since
                    .map(|since| since.elapsed())
                    .unwrap_or_default();
                let mut events = Vec::new();
                if !guarded || elapsed >= PARTIAL_REPLY_FLUSH_GUARD {
                    let drained = {
                        let mut parser = stdin_ansi_parser.lock().unwrap();
                        if guarded {
                            parser.finalize_force()
                        } else {
                            parser.finalize_fast_partial()
                        }
                    };
                    if !drained.is_empty() {
                        events.extend(key_decoder.feed(&drained));
                        if guarded {
                            events.extend(key_decoder.flush_force());
                        }
                    }
                    reply_in_progress_since = None;
                }
                events.extend(key_decoder.flush_timeout());
                if !dispatch_decoded_events(events, &send_input_instructions) {
                    break 'stdin;
                }

                update_finalization_state(
                    &stdin_ansi_parser,
                    &key_decoder,
                    &mut needs_finalization,
                    &mut reply_in_progress_since,
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
}

#[derive(Debug, Clone, PartialEq)]
enum DecodedStdinEvent {
    KittyKey(KeyWithModifier, Vec<u8>),
    Termwiz(InputEvent, Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingInputState {
    None,
    FastTimeout,
    InFlightSequence,
}

const PASTE_START: &[u8] = b"\x1b[200~";
const PASTE_END: &[u8] = b"\x1b[201~";
const MAX_INFLIGHT_SEQ_LEN: usize = 64;

/// Frame input before trying either decoder so they never share a partial sequence.
struct StdinKeyDecoder {
    input_parser: InputParser,
    pending: Vec<u8>,
    last_feed: Instant,
    explicitly_disable_kitty_keyboard_protocol: bool,
}

/// Scan CSI/SS3 framing. Return length and completeness, or wait for more bytes.
fn scan_sequence(bytes: &[u8]) -> Option<(usize, bool)> {
    if bytes[1] == b'O' && bytes.len() > 2 && bytes[2] != b'1' {
        let complete = (0x40..=0x7e).contains(&bytes[2]);
        return Some((if complete { 3 } else { 2 }, complete));
    }
    let mut end = 2;
    while end < bytes.len() && (0x30..=0x3f).contains(&bytes[end]) {
        end += 1;
    }
    if bytes[1] == b'[' {
        while end < bytes.len() && (0x20..=0x2f).contains(&bytes[end]) {
            end += 1;
        }
    }
    if end == bytes.len() {
        return (end > MAX_INFLIGHT_SEQ_LEN).then_some((end, false));
    }
    if (0x40..=0x7e).contains(&bytes[end]) {
        Some((end + 1, true))
    } else {
        Some((end, false))
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

fn trailing_prefix_len(buf: &[u8], marker: &[u8]) -> usize {
    (1..marker.len().min(buf.len() + 1))
        .rev()
        .find(|&len| buf.ends_with(&marker[..len]))
        .unwrap_or(0)
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
    fn new(explicitly_disable_kitty_keyboard_protocol: bool) -> Self {
        Self {
            input_parser: InputParser::new(),
            pending: Vec::new(),
            last_feed: Instant::now(),
            explicitly_disable_kitty_keyboard_protocol,
        }
    }

    fn clear(&mut self) {
        self.pending.clear();
        self.input_parser = InputParser::new();
    }

    fn pending_state(&self) -> PendingInputState {
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

    fn feed(&mut self, bytes: &[u8]) -> Vec<DecodedStdinEvent> {
        self.pending.extend_from_slice(bytes);
        self.last_feed = Instant::now();
        let mut out = Vec::new();
        let mut offset = 0;
        while offset < self.pending.len() {
            let rest = &self.pending[offset..];
            let Some((len, maybe_more)) = self.token_len(rest) else {
                break;
            };
            let token = &rest[..len];
            if let Some(key) = self.try_parse_kitty(token) {
                if !csi_is_kitty_key_release(token) {
                    out.push(DecodedStdinEvent::KittyKey(key, token.to_vec()));
                }
            } else {
                Self::feed_termwiz_slice(&mut self.input_parser, token, maybe_more, &mut out);
            }
            offset += len;
        }
        self.pending.drain(..offset);
        out
    }

    fn flush_timeout(&mut self) -> Vec<DecodedStdinEvent> {
        if self.pending_state() == PendingInputState::FastTimeout
            || (self.pending_state() == PendingInputState::InFlightSequence
                && self.last_feed.elapsed() >= PARTIAL_REPLY_FLUSH_GUARD)
        {
            self.flush_force()
        } else {
            Vec::new()
        }
    }

    fn flush_force(&mut self) -> Vec<DecodedStdinEvent> {
        let mut out = Vec::new();
        if !self.input_parser.is_pasting() {
            Self::feed_termwiz_slice(&mut self.input_parser, &self.pending, false, &mut out);
            self.pending.clear();
        }
        out
    }

    fn feed_termwiz_slice(
        parser: &mut InputParser,
        chunk: &[u8],
        maybe_more: bool,
        out: &mut Vec<DecodedStdinEvent>,
    ) {
        let mut cursor = 0;
        let mut emit = |event, consumed: usize| {
            let end = (cursor + consumed).min(chunk.len());
            out.push(DecodedStdinEvent::Termwiz(
                event,
                chunk[cursor..end].to_vec(),
            ));
            cursor = end;
        };
        parser.parse_with_consumed(chunk, &mut emit, maybe_more);
        if !maybe_more && parser.buffered_len() > 0 && !parser.is_pasting() {
            parser.parse_with_consumed(&[], &mut emit, false);
        }
        if !parser.is_pasting() && parser.buffered_len() > 0 {
            *parser = InputParser::new();
        }
    }

    fn try_parse_kitty(&self, bytes: &[u8]) -> Option<KeyWithModifier> {
        if !self.input_parser.is_pasting()
            && !self.explicitly_disable_kitty_keyboard_protocol
            && bytes.starts_with(b"\x1b[")
        {
            if let KittyParseOutcome::Complete(key) = KittyKeyboardParser::new().feed(bytes) {
                return Some(key);
            }
        }
        None
    }

    fn token_len(&self, bytes: &[u8]) -> Option<(usize, bool)> {
        if self.input_parser.is_pasting() {
            let len = bytes
                .windows(PASTE_END.len())
                .position(|w| w == PASTE_END)
                .map(|pos| pos + PASTE_END.len())
                .unwrap_or_else(|| bytes.len() - trailing_prefix_len(bytes, PASTE_END));
            return (len > 0).then_some((len, true));
        }
        if bytes[0] != 0x1b {
            let len = bytes
                .iter()
                .position(|&b| b == 0x1b)
                .unwrap_or_else(|| bytes.len() - trailing_incomplete_utf8_len(bytes));
            return (len > 0).then_some((len, false));
        }
        if bytes.len() < 2 {
            return None;
        }
        let prefix = usize::from(bytes[1] == 0x1b);
        let sequence = &bytes[prefix..];
        if sequence.len() < 2 {
            return None;
        }
        if matches!(sequence[1], b'[' | b'O') {
            let (len, complete) = scan_sequence(sequence)?;
            if prefix == 1
                && (!complete
                    || &sequence[..len] == PASTE_START
                    || (sequence[..len].ends_with(b"u") || sequence[..len].contains(&b':'))
                        && self.try_parse_kitty(&sequence[..len]).is_some())
            {
                // Kitty-only CSI and paste cannot be Alt-prefixed; legacy keys can.
                return Some((1, false));
            }
            return Some((prefix + len, complete && sequence[1] == b'['));
        }
        if prefix == 1 {
            return Some((1, false));
        }
        let len = 1 + utf8_expected_len(bytes[1]);
        if bytes.len() < len && bytes[2..].iter().all(|&b| (b & 0xc0) == 0x80) {
            None
        } else if bytes.len() >= len && std::str::from_utf8(&bytes[1..len]).is_ok() {
            Some((len, false))
        } else {
            Some((2, false))
        }
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

/// Build the fire-and-forget host-query batch sent at client startup.
/// The host's replies refine `Screen`'s cached state asynchronously as
/// they arrive; the UI does not block on them.
fn build_startup_query_string(
    support_kitty_graphics_protocol: bool,
    query_host_theme: bool,
) -> String {
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
        "\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}\u{1b}_Ga=q,i=32,s=4,v=4,t=d,f=24,o=z;eJxjYCANAAAAMAAB\u{1b}\u{5c}"
    } else {
        ""
    };
    let host_theme_query = if query_host_theme {
        crate::QUERY_HOST_THEME
    } else {
        ""
    };
    format!(
        "{}{}\u{1b}]11;?\u{1b}\u{5c}\u{1b}]10;?\u{1b}\u{5c}\u{1b}[?2026$p{}{}\u{1b}[?2027$p\u{1b}[c",
        host_theme_query, PIXEL_SIZE_QUERY, kitty_graphics_probe, PROGRAM_STATUS_QUERY
    )
}

pub(crate) const PIXEL_SIZE_QUERY: &str = "\u{1b}[14t\u{1b}[16t";
pub(crate) const PROGRAM_STATUS_QUERY: &str = "\u{1b}]7501;?\u{1b}\u{5c}";

#[cfg(test)]
mod tests {
    use super::{build_startup_query_string, extract_focus_reports, PIXEL_SIZE_QUERY};
    use super::{DecodedStdinEvent, PendingInputState, StdinAnsiParser, StdinKeyDecoder};
    use zellij_utils::data::{BareKey, KeyWithModifier};
    use zellij_utils::vendored::termwiz::input::{InputEvent, KeyCode, KeyEvent, Modifiers};

    fn kitty(raw: &[u8], key: KeyWithModifier) -> DecodedStdinEvent {
        DecodedStdinEvent::KittyKey(key, raw.to_vec())
    }

    fn legacy(raw: &[u8], key: KeyCode, modifiers: Modifiers) -> DecodedStdinEvent {
        DecodedStdinEvent::Termwiz(InputEvent::Key(KeyEvent { key, modifiers }), raw.to_vec())
    }

    #[test]
    fn pixel_size_query_probes_text_area_and_character_cell() {
        assert_eq!(PIXEL_SIZE_QUERY, "\u{1b}[14t\u{1b}[16t");
    }

    #[test]
    fn startup_query_has_no_palette_register_loop() {
        let query = build_startup_query_string(true, false);
        assert_eq!(
            query,
            "\u{1b}[14t\u{1b}[16t\u{1b}]11;?\u{1b}\u{5c}\u{1b}]10;?\u{1b}\u{5c}\u{1b}[?2026$p\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}\u{1b}_Ga=q,i=32,s=4,v=4,t=d,f=24,o=z;eJxjYCANAAAAMAAB\u{1b}\u{5c}\u{1b}]7501;?\u{1b}\u{5c}\u{1b}[?2027$p\u{1b}[c"
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
    fn startup_query_leads_with_the_host_theme_query_when_requested() {
        let query = build_startup_query_string(false, true);
        assert!(query.starts_with("\u{1b}[?996n\u{1b}[14t\u{1b}[16t"));
        assert!(query.ends_with("\u{1b}[c"));
    }

    #[test]
    fn startup_query_contains_kitty_probe_before_barrier() {
        let query = build_startup_query_string(true, false);
        assert!(query.contains("\u{1b}_Ga=q,i=31,s=1,v=1,t=d,f=24;AAAA\u{1b}\u{5c}"));
        let probe_pos = query.find("\u{1b}_Ga=q,i=31,").unwrap();
        let barrier_pos = query.find("\u{1b}[c").unwrap();
        assert!(probe_pos < barrier_pos);
    }

    #[test]
    fn startup_query_contains_zlib_probe_between_kitty_probe_and_barrier() {
        let query = build_startup_query_string(true, false);
        let probe_pos = query.find("\u{1b}_Ga=q,i=31,").unwrap();
        let zlib_probe_pos = query
            .find("\u{1b}_Ga=q,i=32,s=4,v=4,t=d,f=24,o=z;eJxjYCANAAAAMAAB\u{1b}\u{5c}")
            .unwrap();
        let barrier_pos = query.find("\u{1b}[c").unwrap();
        assert!(probe_pos < zlib_probe_pos);
        assert!(zlib_probe_pos < barrier_pos);
    }

    #[test]
    fn startup_query_asks_for_grapheme_cluster_mode_right_before_the_barrier() {
        for (kitty, theme) in [(true, true), (false, false)] {
            let query = build_startup_query_string(kitty, theme);
            assert!(query.ends_with("\u{1b}[?2027$p\u{1b}[c"), "{:?}", query);
            assert_eq!(query.matches("\u{1b}[?2027$p").count(), 1);
        }
    }

    #[test]
    fn startup_query_omits_kitty_probe_when_the_protocol_is_disabled() {
        let query = build_startup_query_string(false, false);
        assert!(!query.contains("\u{1b}_G"));
        assert!(query.ends_with("\u{1b}[c"));
        assert!(query.starts_with("\u{1b}[14t\u{1b}[16t"));
    }

    #[test]
    fn coalesced_kitty_repeats_survive_every_chunk_size() {
        let stream = b"\x1b[107;5u".repeat(12);
        let expected = vec![
            kitty(
                b"\x1b[107;5u",
                KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier()
            );
            12
        ];
        for chunk_size in 1..=stream.len() {
            let mut ansi = StdinAnsiParser::new();
            let mut dec = StdinKeyDecoder::new(false);
            let mut events = Vec::new();
            for chunk in stream.chunks(chunk_size) {
                events.extend(dec.feed(&ansi.feed(chunk).residue));
            }
            assert_eq!(events, expected, "chunk size {chunk_size}");
        }
    }

    #[test]
    fn split_csi_waits_past_fast_timeout_but_not_forever() {
        let mut dec = StdinKeyDecoder::new(false);
        assert!(dec.feed(b"\x1b[1").is_empty());
        dec.last_feed -= super::LONE_ESC_FLUSH_INTERVAL;
        assert!(dec.flush_timeout().is_empty());
        let expected = kitty(
            b"\x1b[107;5u",
            KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier(),
        );
        assert_eq!(
            dec.feed(b"07;5u\x1b[107;5u"),
            vec![expected.clone(), expected]
        );
        assert!(dec.feed(b"\x1b[1").is_empty());
        dec.last_feed -= super::PARTIAL_REPLY_FLUSH_GUARD;
        assert_eq!(
            dec.flush_timeout(),
            vec![
                legacy(b"\x1b[", KeyCode::Char('['), Modifiers::ALT),
                legacy(b"1", KeyCode::Char('1'), Modifiers::NONE),
            ]
        );
        assert_eq!(
            dec.feed(b"a"),
            vec![legacy(b"a", KeyCode::Char('a'), Modifiers::NONE)]
        );
    }

    #[test]
    fn kitty_subparameters_preserve_press_repeat_and_shifted_keys_but_ignore_release() {
        let mut dec = StdinKeyDecoder::new(false);
        let ctrl_k = KeyWithModifier::new(BareKey::Char('k')).with_ctrl_modifier();
        assert_eq!(
            dec.feed(b"\x1b[107;5:1;107u\x1b[107;5:2u\x1b[107;5:3u\x1b[97;1:3u\x1b[91:123;4u"),
            vec![
                kitty(b"\x1b[107;5:1;107u", ctrl_k.clone()),
                kitty(b"\x1b[107;5:2u", ctrl_k),
                kitty(
                    b"\x1b[91:123;4u",
                    KeyWithModifier::new(BareKey::Char('['))
                        .with_alt_modifier()
                        .with_shift_modifier()
                ),
            ]
        );
    }

    #[test]
    fn mixed_text_kitty_and_mouse_keep_their_own_bytes() {
        let mut dec = StdinKeyDecoder::new(false);
        let events = dec.feed(b"a\x1b[91;3u\x1b[<35;10;20M\x1b[93;3u");
        assert_eq!(events.len(), 4);
        assert_eq!(events[0], legacy(b"a", KeyCode::Char('a'), Modifiers::NONE));
        assert_eq!(
            events[1],
            kitty(
                b"\x1b[91;3u",
                KeyWithModifier::new(BareKey::Char('[')).with_alt_modifier()
            )
        );
        assert!(
            matches!(&events[2], DecodedStdinEvent::Termwiz(InputEvent::Mouse(_), raw) if raw == b"\x1b[<35;10;20M")
        );
        assert_eq!(
            events[3],
            kitty(
                b"\x1b[93;3u",
                KeyWithModifier::new(BareKey::Char(']')).with_alt_modifier()
            )
        );
    }

    #[test]
    fn paste_keeps_literal_kitty_sequences_and_waits_for_split_end_marker() {
        let mut dec = StdinKeyDecoder::new(false);
        assert!(dec.feed(b"\x1b[200~literal \x1b[107;5u\x1b[20").is_empty());
        dec.last_feed -= super::PARTIAL_REPLY_FLUSH_GUARD;
        assert!(dec.flush_timeout().is_empty());
        let events = dec.feed(b"1~\x1b[91;3u");
        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], DecodedStdinEvent::Termwiz(InputEvent::Paste(s), _) if s == "literal \x1b[107;5u")
        );
        assert_eq!(
            events[1],
            kitty(
                b"\x1b[91;3u",
                KeyWithModifier::new(BareKey::Char('[')).with_alt_modifier()
            )
        );
    }

    #[test]
    fn lone_esc_legacy_alt_keys_and_fragmented_utf8_still_work() {
        for disabled in [false, true] {
            let mut dec = StdinKeyDecoder::new(disabled);
            assert!(dec.feed(b"\x1b").is_empty());
            assert_eq!(
                dec.flush_timeout(),
                vec![legacy(b"\x1b", KeyCode::Escape, Modifiers::NONE)]
            );
            assert_eq!(
                dec.feed(b"\x1b\x1b[A\x1b\x1b[5~\x1b\x1bOA"),
                vec![
                    legacy(b"\x1b\x1b[A", KeyCode::UpArrow, Modifiers::ALT),
                    legacy(b"\x1b\x1b[5~", KeyCode::PageUp, Modifiers::ALT),
                    legacy(b"\x1b\x1bOA", KeyCode::ApplicationUpArrow, Modifiers::ALT),
                ]
            );
            assert!(dec.feed(b"\xc3").is_empty());
            assert_eq!(
                dec.feed(b"\xa9"),
                vec![legacy("é".as_bytes(), KeyCode::Char('é'), Modifiers::NONE)]
            );
        }
    }

    #[test]
    fn unknown_csi_does_not_wedge_following_keys() {
        let mut dec = StdinKeyDecoder::new(false);
        assert_eq!(
            dec.feed(b"\x1b[?1u\x1b[<ua"),
            vec![legacy(b"a", KeyCode::Char('a'), Modifiers::NONE)]
        );
        assert_eq!(dec.pending_state(), PendingInputState::None);
    }
}
