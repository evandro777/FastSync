//! `rules.fastsyncignore` in the synced folder: gitignore syntax, like Dropbox's `rules.dropboxignore`.
//! Own small matcher instead of the `ignore`/`globset` crates, which would pull `regex` into a light app.

/// The rules file, in the root of the synced folder. It is synced like any file, so every machine shares the rules.
pub const RULES: &str = "rules.fastsyncignore";

#[derive(Default, Clone, Debug, PartialEq)]
pub struct Rules(Vec<Rule>);

#[derive(Clone, Debug, PartialEq)]
struct Rule {
    glob: Vec<char>,
    negate: bool,
    dir_only: bool,
}

impl Rules {
    /// The gitignore subset: comments, `!`, trailing `/`, leading or inner `/` anchors, `*`, `?`, `[a-z]`, `**`.
    pub fn parse(text: &str) -> Self {
        let mut rules = Vec::new();
        // Windows editors may save a BOM, which would otherwise stick to the first rule and make it match nothing.
        for line in text.trim_start_matches('\u{feff}').lines() {
            // Trailing spaces are dropped unless escaped; `\ ` is rare enough to leave out.
            let mut s = line.trim_end();
            if s.is_empty() || s.starts_with('#') {
                continue;
            }
            let negate = s.starts_with('!');
            if negate {
                s = &s[1..];
            }
            let dir_only = s.ends_with('/');
            let s = s.trim_end_matches('/');
            if s.is_empty() {
                continue;
            }
            let anchored = s.contains('/');
            let s = s.strip_prefix('/').unwrap_or(s);
            let glob = if anchored { s.to_string() } else { format!("**/{s}") };
            rules.push(Rule {
                glob: glob.chars().collect(),
                negate,
                dir_only,
            });
        }
        Self(rules)
    }

    /// True when the file `path` (relative, `/`-separated) or a folder above it is ignored.
    pub fn matches(&self, path: &str) -> bool {
        self.matches_as(path, false)
    }

    /// `matches` for a folder: a rule with a trailing `/` (`build/`) hides it too, so a walk can skip it whole.
    pub fn matches_dir(&self, path: &str) -> bool {
        self.matches_as(path, true)
    }

    fn matches_as(&self, path: &str, last_is_dir: bool) -> bool {
        if self.0.is_empty() || path == RULES {
            return false;
        }
        // A file cannot be re-included once its folder is excluded, so the first ignored level decides.
        let ends: Vec<usize> = path.match_indices('/').map(|(i, _)| i).collect();
        ends.iter()
            .map(|&i| (&path[..i], true))
            .chain(std::iter::once((path, last_is_dir)))
            .any(|(p, is_dir)| self.ignored_at(p, is_dir))
    }

    fn ignored_at(&self, path: &str, is_dir: bool) -> bool {
        let t: Vec<char> = path.chars().collect();
        let mut ignored = false;
        for r in &self.0 {
            if (is_dir || !r.dir_only) && glob(&r.glob, &t, &mut Failed::new()) {
                ignored = !r.negate;
            }
        }
        ignored
    }
}

/// What a change of the rules can do to the names they hide (`Rules::change`).
#[derive(Debug, PartialEq)]
pub enum Change {
    /// Only hiding rules were added (or nothing changed): nothing that was hidden is visible now, so no cloud or local
    /// listing is needed, only the synced files that now match.
    HidesMore,
    /// Anchored folders or files (`gen/api`, no wildcard) lost their hiding rule: what lies under these names may be
    /// visible now, nothing else.
    Unhides(Vec<String>),
}

impl Rules {
    /// How `new` differs from `self` (the old rules), or `None` when that cannot be said without looking at everything
    /// (an un-hidden name that is not a literal path, a rule that was reordered or turned into a `!`). The last matching
    /// rule decides, so order matters: an added hiding rule can never show a name, a removed one shows only what it
    /// matched, and anything else is a full scan.
    pub fn change(&self, new: &Rules) -> Option<Change> {
        if let Some(added) = extra(&self.0, &new.0) {
            return added.iter().all(|r| !r.negate).then_some(Change::HidesMore);
        }
        let removed = extra(&new.0, &self.0)?;
        let literal =
            |r: &Rule| !r.negate && !r.glob.iter().any(|c| matches!(c, '*' | '?' | '[' | '\\')) && !r.glob.is_empty();
        removed
            .iter()
            .all(|r| literal(r))
            .then(|| Change::Unhides(removed.iter().map(|r| r.glob.iter().collect()).collect()))
    }
}

/// The rules of `big` that are left when `small` is taken out in order, or `None` when `small` is not in `big` in that
/// order. Greedy matching finds a subsequence whenever there is one.
fn extra<'a>(small: &[Rule], big: &'a [Rule]) -> Option<Vec<&'a Rule>> {
    let mut want = small.iter().peekable();
    let mut left = Vec::new();
    for r in big {
        if want.peek() == Some(&r) {
            want.next();
        } else {
            left.push(r);
        }
    }
    want.peek().is_none().then_some(left)
}

/// (pattern left, text left) pairs that are known not to match. Every call below gets a suffix of the same pattern and
/// text, so the two lengths name the state exactly.
type Failed = std::collections::HashSet<(usize, usize)>;

/// `*` and `?` stop at `/`; `**/` is any number of folders (also none); other `**` crosses `/`.
fn glob(p: &[char], t: &[char], failed: &mut Failed) -> bool {
    let Some((&c, rest)) = p.split_first() else {
        return t.is_empty();
    };
    match c {
        '*' if rest.first() == Some(&'*') => {
            let after = &rest[1..];
            if let Some(tail) = after.strip_prefix(&['/']) {
                sub(tail, t, failed) || (0..t.len()).any(|i| t[i] == '/' && sub(tail, &t[i + 1..], failed))
            } else {
                (0..=t.len()).any(|i| sub(after, &t[i..], failed))
            }
        }
        '*' => star_ok(rest, t, failed),
        '?' => t.first().is_some_and(|&x| x != '/') && glob(rest, &t[1..], failed),
        '[' => match class(rest) {
            Some((set, neg, after)) => {
                t.first().is_some_and(|&x| x != '/' && set_has(set, x) != neg) && glob(after, &t[1..], failed)
            }
            None => t.first() == Some(&'[') && glob(rest, &t[1..], failed),
        },
        '\\' if !rest.is_empty() => t.first() == Some(&rest[0]) && glob(&rest[1..], &t[1..], failed),
        _ => t.first() == Some(&c) && glob(rest, &t[1..], failed),
    }
}

/// A `glob` call made from a place that tries many splits. With several `*` the same (pattern, text) state is reached
/// again and again, which took seconds for eight of them on one 40-letter name: a state that failed is remembered.
/// A pattern with fewer than two `*` left cannot repeat work, so rules like `*.log` never touch the set.
fn sub(p: &[char], t: &[char], failed: &mut Failed) -> bool {
    if p.iter().filter(|&&c| c == '*').count() < 2 {
        return glob(p, t, failed);
    }
    let key = (p.len(), t.len());
    if failed.contains(&key) {
        return false;
    }
    let found = glob(p, t, failed);
    if !found {
        failed.insert(key);
    }
    found
}

/// `*` must not swallow a `/`: tries every split but rejects those whose skipped part holds one.
fn star_ok(rest: &[char], t: &[char], failed: &mut Failed) -> bool {
    (0..=t.len())
        .take_while(|&i| i == 0 || t[i - 1] != '/')
        .any(|i| sub(rest, &t[i..], failed))
}

/// `[abc]`, `[!a-c]` after the `[`: (members, negated, what follows the `]`).
fn class(p: &[char]) -> Option<(&[char], bool, &[char])> {
    let neg = matches!(p.first(), Some('!' | '^'));
    let body = if neg { &p[1..] } else { p };
    // A `]` right after the opening is a member, as in POSIX.
    let end = body.iter().skip(1).position(|&c| c == ']')? + 1;
    Some((&body[..end], neg, &body[end + 1..]))
}

fn set_has(set: &[char], x: char) -> bool {
    let mut i = 0;
    while i < set.len() {
        if i + 2 < set.len() && set[i + 1] == '-' {
            if (set[i]..=set[i + 2]).contains(&x) {
                return true;
            }
            i += 3;
        } else {
            if set[i] == x {
                return true;
            }
            i += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(rules: &str, path: &str) -> bool {
        Rules::parse(rules).matches(path)
    }

    #[test]
    fn a_byte_order_mark_does_not_disable_the_first_rule() {
        assert!(m("\u{feff}*.log\r\nbuild/\r\n", "x/a.log") && m("\u{feff}*.log\r\nbuild/\r\n", "build/o"));
    }

    #[test]
    fn comments_blanks_and_empty_rules_match_nothing() {
        assert!(!m("# *.log\n\n   \n/\n!\n", "a.log"));
    }

    #[test]
    fn plain_patterns_match_at_any_depth() {
        assert!(m("*.log", "a.log") && m("*.log", "x/y/a.log"));
        assert!(!m("*.log", "a.log.txt") && !m("*.log", "alog"));
        assert!(m("node_modules", "a/node_modules/x.js"));
        assert!(m("a?c", "abc") && !m("a?c", "a/c"));
        assert!(m("[a-c]x", "bx") && !m("[a-c]x", "dx") && m("[!a-c]x", "dx"));
    }

    #[test]
    fn a_slash_anchors_to_the_root() {
        assert!(m("/top.txt", "top.txt") && !m("/top.txt", "a/top.txt"));
        assert!(m("docs/x.md", "docs/x.md") && !m("docs/x.md", "a/docs/x.md"));
        assert!(!m("docs/*.md", "docs/a/b.md"));
    }

    #[test]
    fn double_star_crosses_folders() {
        assert!(m("docs/**/*.md", "docs/a.md") && m("docs/**/*.md", "docs/a/b/c.md"));
        assert!(!m("docs/**/*.md", "other/a.md"));
        assert!(m("**/cache", "cache/x") && m("**/cache", "a/b/cache/x"));
        assert!(m("logs/**", "logs/a/b") && !m("logs/**", "logs"));
    }

    #[test]
    fn a_trailing_slash_matches_folders_only() {
        assert!(m("build/", "build/out.bin") && m("build/", "a/build/out.bin"));
        assert!(!m("build/", "build"));
    }

    #[test]
    fn a_change_of_rules_says_what_it_can_show_or_asks_for_everything() {
        let change = |old: &str, new: &str| Rules::parse(old).change(&Rules::parse(new));
        // Only hiding rules added (anywhere), or only a comment changed: nothing becomes visible.
        assert_eq!(change("*.log\n", "*.log\n*.tmp\n"), Some(Change::HidesMore));
        assert_eq!(
            change("a/b\n!keep\n", "x\na/b\n*.tmp\n!keep\n"),
            Some(Change::HidesMore)
        );
        assert_eq!(change("*.log\n", "# note\n*.log\n"), Some(Change::HidesMore));
        // A literal anchored rule removed: only what lies under that name can show.
        assert_eq!(
            change("gen/api\n/build/\nx\n", "x\n"),
            Some(Change::Unhides(vec!["gen/api".into(), "build".into()]))
        );
        // Everything else needs the whole cloud: a wildcard or unanchored name removed, a `!` added or removed, a
        // reorder (the last rule wins: the same lines can show a file that was hidden), a mix.
        assert_eq!(change("*.log\n", ""), None);
        assert_eq!(change("node_modules/\n", ""), None);
        assert_eq!(change("", "!keep\n"), None);
        assert_eq!(change("a/b\n!a/b/c\n", "a/b\n"), None);
        assert_eq!(change("!keep.log\n*.log\n", "*.log\n!keep.log\n"), None);
        assert_eq!(change("a/b\n", "c/d\n"), None);
    }

    #[test]
    fn negation_and_last_rule_wins_but_not_below_an_ignored_folder() {
        assert!(!m("*.log\n!keep.log", "keep.log") && m("*.log\n!keep.log", "other.log"));
        assert!(m("*.log\n!keep.log\n*.log", "keep.log"));
        assert!(m("out/\n!out/a.txt", "out/a.txt"));
    }

    #[test]
    fn the_rules_file_itself_is_never_ignored() {
        assert!(!m("*", RULES) && m("*", "a.txt"));
    }

    #[test]
    fn escapes_and_unclosed_class_are_literal() {
        assert!(m("\\#x", "#x") && m("\\!x", "!x"));
        assert!(m("a[b", "a[b"));
    }

    #[test]
    fn many_stars_do_not_blow_up() {
        let name = "a".repeat(60);
        let start = std::time::Instant::now();
        assert!(!m("*a*a*a*a*a*a*a*a*b", &name));
        assert!(m("*a*a*a*a*a*a*a*a*", &name));
        assert!(!m("**/*a*a*a*a*a*a*a*b", &format!("{name}/{name}")));
        assert!(start.elapsed().as_secs() < 1, "took {:?}", start.elapsed());
    }

    /// The matcher before the failed-state set: slow with many stars, but plainly correct.
    fn reference(p: &[char], t: &[char]) -> bool {
        let Some((&c, rest)) = p.split_first() else {
            return t.is_empty();
        };
        match c {
            '*' if rest.first() == Some(&'*') => {
                let after = &rest[1..];
                if let Some(tail) = after.strip_prefix(&['/']) {
                    reference(tail, t) || (0..t.len()).any(|i| t[i] == '/' && reference(tail, &t[i + 1..]))
                } else {
                    (0..=t.len()).any(|i| reference(after, &t[i..]))
                }
            }
            '*' => (0..=t.len())
                .take_while(|&i| i == 0 || t[i - 1] != '/')
                .any(|i| reference(rest, &t[i..])),
            '?' => t.first().is_some_and(|&x| x != '/') && reference(rest, &t[1..]),
            '[' => match class(rest) {
                Some((set, neg, after)) => {
                    t.first().is_some_and(|&x| x != '/' && set_has(set, x) != neg) && reference(after, &t[1..])
                }
                None => t.first() == Some(&'[') && reference(rest, &t[1..]),
            },
            '\\' if !rest.is_empty() => t.first() == Some(&rest[0]) && reference(&rest[1..], &t[1..]),
            _ => t.first() == Some(&c) && reference(rest, &t[1..]),
        }
    }

    #[test]
    fn remembering_failed_states_never_changes_an_answer() {
        // A tiny alphabet, so stars, classes, escapes and slashes meet often.
        let (pat, txt): (Vec<char>, Vec<char>) = ("ab/**?[]!-\\".chars().collect(), "ab/-[]*".chars().collect());
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut next = |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        for _ in 0..30_000 {
            let p: Vec<char> = (0..next(9)).map(|_| pat[next(pat.len())]).collect();
            let t: Vec<char> = (0..next(9)).map(|_| txt[next(txt.len())]).collect();
            assert_eq!(
                glob(&p, &t, &mut Failed::new()),
                reference(&p, &t),
                "pattern {p:?} text {t:?}"
            );
        }
    }
}
