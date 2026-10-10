import json

from hpc_bridge.cost import cut_output, cut_streams, estimate_spend


def test_estimate_spend_node_hours():
    # 1 node held 1 hour at charge_factor 1.0 => 1 node-hour
    assert estimate_spend(3600.0, nodes=1, charge_factor=1.0) == 1.0
    assert estimate_spend(1800.0, nodes=2, charge_factor=1.0) == 1.0
    assert estimate_spend(3600.0, nodes=1, charge_factor=0.0) == 0.0


# --- the output cut: a long stream keeps its END, opening with a marker that says what was dropped ---

def _numbered(n):
    return "".join(f"line {i}\n" for i in range(1, n + 1))


def test_cut_output_passes_output_within_the_cap_through_unchanged():
    assert cut_output("short\n", 100, stream="stdout") == ("short\n", False)
    exact = "x" * 99 + "\n"
    assert cut_output(exact, 100, stream="stdout") == (exact, False)  # at the cap is not over it


def test_cut_output_keeps_the_tail_from_a_whole_line_with_exact_counts():
    text = _numbered(2000)  # "line 1\n" … "line 2000\n": 18,893 chars, the last ten lines 10 chars each
    out, cut = cut_output(text, 105, stream="stdout")
    marker, kept = out.split("\n", 1)
    assert cut
    # the END is kept, from a whole line: 105 chars from the end lands mid-"line 1990", so it starts at "line 1991"
    assert kept == _numbered(2000)[len(_numbered(1990)):]
    assert kept.startswith("line 1991\n") and kept.endswith("line 2000\n")
    assert marker == ("[hpc-bridge: stdout too long — showing only its last 10 lines (100 chars); "
                      "1,990 lines (18,793 chars) before them were dropped]")
    assert len(_numbered(1990)) == 18_793  # the dropped count is exact: every char not shown


def test_cut_output_a_single_line_longer_than_the_cap_is_shown_from_its_middle():
    out, cut = cut_output("a" * 40 + "b" * 10, 10, stream="stdout")
    assert cut
    assert out == ("[hpc-bridge: stdout too long — showing only its last 10 chars (1 line, starting mid-line); "
                   "40 chars before it were dropped]\n" + "b" * 10)


def test_cut_output_marker_grammar_for_one_line_or_char():
    out, _ = cut_output("\n" + "x" * 100, 100, stream="stdout")
    assert out.startswith("[hpc-bridge: stdout too long — showing only its last 1 line (100 chars); "
                          "1 line (1 char) before it was dropped]\n")
    out, _ = cut_output("a" + "b" * 10, 10, stream="stdout")
    assert out.startswith("[hpc-bridge: stdout too long — showing only its last 10 chars (1 line, starting mid-line); "
                          "1 char before it was dropped]\n")


def test_cut_output_does_not_trade_most_of_the_cap_for_a_whole_line():
    # a one-line JSON result, then a short trailer: starting at the next whole line would keep 13 of 16,000 chars
    blob = json.dumps({"results": ["x" * 40] * 500})  # ~22,000 chars on one line
    text = "starting\n" + blob + "\n" + "done in 3.2s\n"
    out, cut = cut_output(text, 16_000, stream="stdout")
    marker, kept = out.split("\n", 1)
    assert cut
    assert kept == text[-16_000:] and kept.endswith('"]}\ndone in 3.2s\n')  # all of the cap, cut mid-line
    assert marker == ("[hpc-bridge: stdout too long — showing only its last 16,000 chars (2 lines, the first starting "
                      f"mid-line); {len(text) - 16_000:,} chars before them were dropped]")
    # a cut point within half the cap of a line start still moves to that whole line
    lines = "".join(f"{i:05d}" + "y" * 94 + "\n" for i in range(300))  # 100-char lines
    out, _ = cut_output(lines, 1_050, stream="stdout")
    assert out.split("\n", 1)[1] == lines[-1_000:]


def test_cut_output_counts_at_least_when_the_sdk_may_already_have_cut():
    # the received text is exactly the SDK's snippet_lines long: it may itself be a tail -> "at least"
    text = _numbered(2000)
    out, cut = cut_output(text, 105, stream="stdout", sdk_lines=2000)
    assert cut
    assert "at least 1,990 lines (at least 18,793 chars) before them were dropped]" in out
    assert out.endswith("line 2000\n")
    # one line short of it: the SDK cut nothing, so the counts are exact
    exact, _ = cut_output(_numbered(1999), 105, stream="stdout", sdk_lines=2000)
    assert "at least" not in exact and "1,989 lines" in exact


def test_cut_output_marks_an_sdk_cut_within_the_char_cap():
    # short lines: the text fits the char cap, but at the SDK's line limit it may be the SDK's silent tail
    out, cut = cut_output("ab\n" * 30, 1000, stream="stderr", sdk_lines=30)
    assert cut
    assert out == ("[hpc-bridge: stderr too long — showing only its last 30 lines (90 chars); the worker returns at "
                   "most 30 lines, so earlier ones may have been dropped (how many is unknown)]\n" + "ab\n" * 30)
    assert cut_output("ab\n" * 29, 1000, stream="stderr", sdk_lines=30) == ("ab\n" * 29, False)


def test_cut_streams_notice_names_the_cut_stream_and_the_remedy():
    big = _numbered(2000)
    out, err, notice = cut_streams("ok\n", "warn\n", 100)
    assert (out, err, notice) == ("ok\n", "warn\n", None)  # nothing cut: unchanged, no notice
    out, err, notice = cut_streams("ok\n", big, 100)
    assert out == "ok\n" and err.startswith("[hpc-bridge: stderr too long")
    assert notice and notice.startswith("stderr too long for one result: only the end is kept")
    assert "> out.log 2>&1" in notice and "sed -n" in notice and "tail -n" in notice  # write it to a file, read ranges
    _, _, both = cut_streams(big, big, 100)
    assert both and both.startswith("stdout and stderr too long")
