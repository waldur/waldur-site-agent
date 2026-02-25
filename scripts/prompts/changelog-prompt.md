You are generating a changelog entry for waldur-site-agent version {VERSION} (previous: {PREV_VERSION}, date: {DATE}).

Output ONLY the changelog entry in markdown. Do not include any preamble, explanation, or code fences. The output will be inserted directly into CHANGELOG.md.

## Output format

Start with exactly this header:

```
## {VERSION} - {DATE}
```

Then include these sections in order:

### 1. Highlights

A short paragraph (2-4 sentences) explaining WHY this release matters. Focus on user-visible impact, not implementation details. What can operators do now that they couldn't before? What got more reliable?

### 2. What's New / Improvements / Bug Fixes

Group changes by theme. Use subsections like:

- **What's New** - genuinely new capabilities
- **Improvements** - enhancements to existing features
- **Bug Fixes** - corrections (only if there are meaningful ones)

Each item should be a single bullet point. Prefix with the relevant plugin or component in bold when it clarifies scope, e.g.:

- **SLURM**: Fix association limit handling when values are unchanged.
- **Federation**: Add non-blocking order processing via STOMP.
- **Core**: Improve STOMP reconnection with exponential backoff.

### 3. Statistics

A brief summary line, e.g.:

> N commits, M files changed (+A/-R lines)

End the entry with a `---` separator line.

## Rules

1. **Collapse revert pairs**: If a commit was reverted and re-applied, mention only the final state.
2. **Exclude noise**: Skip version bump commits, merge commits, CI-only changes, and trivial reformatting.
3. **No invented information**: Only describe what you can see in the commit data. Do not speculate.
4. **Keep it concise**: Aim for 20-40 lines total. Group small related changes into single bullet points.
5. **Use sentence case**: Start each bullet with a capital letter, end with a period.
6. **Preserve issue references**: Keep Jira/issue keys like [HPCMP-438] or [WAL-9725] in parentheses at the end of the relevant bullet point.
7. **Deduplicate**: If two commits clearly address the same change (e.g., fix + follow-up fix), combine them into one bullet.
