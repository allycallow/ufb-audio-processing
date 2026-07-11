Here is the updated markdown with a new section added to enforce checking and using the correct Node.js version via .nvmrc and nvm use.

Markdown
# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Environment & Runtime

**Ensure toolchain consistency before execution.**

- Always check for the presence of a `.nvmrc` file in the root directory.
- Before installing dependencies, running scripts, or building the project, run `nvm use` to ensure the correct Node.js version is active.
- If `nvm use` fails due to a missing local version, stop and log that `nvm install` is required.

## 3. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 4. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 5. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
[Step] → verify: [check]

[Step] → verify: [check]

[Step] → verify: [check]

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 6. TypeScript Style Guide

**Strict typing and modern TypeScript conventions.**

- **Strict Typing:** Adhere to `strict: true` compiler rules. Explicitly type all variables, parameters, and return types where inference isn't structural or explicitly clear.
- **Functions & Methods:** Every function, method, and arrow function must include explicit parameter types and a return type annotation.
- **Modern Syntax:** Prefer modern ESNext and TypeScript features. Use standard utility types like `Record<string, unknown>`, and union types like `string | null` or `string | undefined`.
- **Specificity:** Avoid using `any`. If a type is genuinely dynamic or unknown, use `unknown` and narrow it downstream using type guards or conditional blocks. Use `is` predicates for custom type guards.
- **Interfaces vs. Types:** Use `interface` for structural object definitions that benefit from declaration merging or extensions, and `type` for aliases, unions, intersections, or primitives.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to