# General AI Notes

## The Golden Rule

When unsure about implementation details, ALWAYS ask the developer.

### Guidelines

- Add specially formatted anchor comments throughout the codebase, where appropriate, for yourself as inline knowledge that can be easily `grep`'d for.
- Use `AIDEV-NOTE:`, `AIDEV-TODO:`, or `AIDEV-QUESTION:` (all-caps prefix) for comments aimed at AI and developers.
- **Important:** Before scanning files, always first try to **grep for existing anchors** `AIDEV-*` in relevant subdirectories.
- **Update relevant anchors** when modifying associated code.
- **Do not remove `AIDEV-NOTE`s** without explicit human instruction.
- Make sure to add relevant anchor comments, whenever a file or piece of code is:
  - too complex, or
  - very important, or
  - confusing, or
  - could have a bug
- If I tell you that you are wrong, think about whether or not you think that's true and respond with facts.
- Avoid apologizing or making conciliatory statements.
- It is not necessary to agree with the user with statements such as "You're right" or "Yes".
- Avoid hyperbole and excitement, stick to the task at hand and complete it pragmatically.

## What AI Must NEVER Do

1. **Never modify test files** - Tests encode human intent
2. **Never change API contracts** - Breaks real applications
3. **Never alter migration files** - Data loss risk
4. **Never commit secrets** - Use environment variables
5. **Never assume business logic** - Always ask
6. **Never remove AIDEV- comments** - They're there for a reason

Remember: We optimize for maintainability over cleverness. When in doubt, choose the boring solution.

# Technical Notes

- Use Gleam 1.12 and Erlang 28.0:
  - For Gleam especially, always verify you produce code that works with the same minor version.
- Verify current dependencies in relevant `gleam.toml` files before assuming that a given library or framework is used.
  - For each dependency, always verify you check the documentation/examples for the same major/minor version we are using.
  - Use https://hexdocs.pm/ for documentation.
- Ensure code quality tools pass after all changes:
  - Ensure all code compiles with no warnings:
    - `gleam build && gleam run -m gleescript`
  - Ensure all code has non-deprecated and valid format:
    - `gleam fix && gleam format`
  - Run type check
    - `gleam gleam check`
