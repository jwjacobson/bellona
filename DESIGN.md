# Bellona — Visual Design

## Color Palette

| Role | Tailwind | Hex |
|------|----------|-----|
| Main background | `stone-700` | `#44403c` |
| Dark background | `stone-800` | `#292524` |
| Text | `stone-100` | `#f5f5f4` |
| Accent | `emerald-700` | `#15803d` |
| Secondary | `olive-900` | `#1a2e05` |
| Info | `teal-400` | `#2dd4bf` |
| Warning | `amber-500` | `#f59e0b` |
| Danger | `red-400` | `#f87171` |

## Design Principles

**No rounded corners.** Use `rounded-none` explicitly if needed to override Tailwind defaults. All elements — buttons, inputs, cards, modals — are sharp-edged.

**No gradients.** Solid color blocks only. No `bg-gradient-*` utilities.

**No shadows.** Use borders or background color contrast to create separation between elements instead.

**Minimal animation.** Only add transitions or animations when they make an interaction meaningfully clearer — e.g. a loading spinner, a state change that would otherwise be invisible. No decorative motion.

**Everything visible.** Prefer showing content directly over hiding it behind dropdowns, hamburger menus, or collapsed sections. Use judgment for genuinely complex UIs, but default to open.

**Flat, geometric layouts.** Think Bauhaus — strong grid, clear blocks of color, content-first. Let the structure do the visual work.