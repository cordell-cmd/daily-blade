# The Daily Blade — Visual Style Guide

> Reference this file whenever making changes or additions to the site.
> The goal is to preserve the gothic pulp fantasy newspaper aesthetic across all pages and features.

---

## Aesthetic & Tone

Gothic pulp fantasy newspaper. Dark, aged, and dramatic — like a broadsheet printed in a cursed medieval city. Every element should feel worn, authoritative, and slightly ominous. Think candlelight, old vellum, dried blood, and gold leaf.

- **Never** introduce bright colours, clean whites, or modern/flat design patterns
- **Never** use sans-serif fonts
- All new UI elements should feel like they belong in a 1920s occult newspaper set in a fantasy world
- Imagery and language should lean dramatic and archaic

---

## Colour Palette

All colours are defined as CSS custom properties on `:root` in index.html. Always use these variables — never hardcode hex values.

| Variable       | Hex       | Usage                                              |
|----------------|-----------|----------------------------------------------------|
| `--ink`        | `#1a0d05` | Page background. Near-black, warm brown-black.     |
| `--parchment`  | `#f4e8cf` | Primary text and light surfaces.                   |
| `--aged`       | `#e2d0a8` | Secondary text, muted labels, inactive states.     |
| `--blood`      | `#8b1a1a` | Danger, death, strong emphasis, active accents.    |
| `--gold`       | `#c9922a` | Borders, highlights, decorative accents, icons.    |
| `--darkgold`   | `#7a4f0d` | Hover states, deeper gold shadows.                 |
| `--shadow`     | `#2a1505` | Card and panel backgrounds (slightly lighter than ink). |
| `--muted`      | `#5c3d1e` | Placeholder text, deeply faded elements.           |

---

## Typography

Two fonts are loaded from Google Fonts. Do not introduce any other typefaces.

- **Cinzel** — Used for all headings, labels, navigation, filter buttons, badges, and any ALL-CAPS display text. Wide letter-spacing (0.3em or more). Always bold or semi-bold.
- **IM Fell English** — Used for all body copy, story text, descriptions, and paragraphs. Serif with an old-world quality. Supports italic for emphasis.

### Text Rules
- Eyebrow labels and category tags: Cinzel, all-caps, letter-spacing 0.3–0.5em
- Story titles: Cinzel or IM Fell English italic, large
- Story body: IM Fell English, comfortable line-height (1.7+)
- Drop caps: large floated first letter on story openings
- Never use `font-weight: 400` on Cinzel — use 700 or 900

---

## Layout & Structure

- Max content width: **1100px**, centred, with generous side padding
- Background is always `--ink`. No light-mode or alternate themes.
- The page has a faint repeating SVG texture overlay (the `body::before` pseudo-element) — preserve this on any new pages
- Sections are separated by `--gold` double borders or ornamental dividers

---

## UI Components

### Cards (Stories & Codex)
- Background: `--shadow`
- Border: 1px `--gold` (or faded variant)
- Corner accents: small gold pseudo-element squares on `::before` / `::after`
- Text: `--parchment` on `--shadow`

### Buttons & Filter Pills
- Default: `--shadow` background, `--aged` text, `--gold` border
- Active / selected: `--blood` or `--gold` background, `--parchment` text
- Font: Cinzel, all-caps, letter-spacing 0.2em+
- Never use rounded pill shapes — prefer sharp or very slightly rounded corners (2–4px)

### Badges
- Small inline tags for entity types, story categories, status
- Background: semi-transparent dark, coloured border or text
- Font: Cinzel, tiny, all-caps

### Dividers & Ornaments
- Use `×  ✦  ×` or `───` style text ornaments as section separators
- Gold horizontal rules (`border-top: 1px solid var(--gold)`)
- Double borders for major section breaks (`border-bottom: 3px double var(--gold)`)

### Masthead
- Centred, full-width
- Eyebrow line: Cinzel, all-caps, small, gold — format: `EST. ANNO DOMINI MMXXVI — DAILY EDITION`
- Title: Cinzel Decorative, very large, `--parchment` with `--gold` text-shadow
- Subtitle: IM Fell English italic, `--aged`
- Date line: IM Fell English italic, `--gold`

---

## Navigation & Tabs

- Main tabs: TALES / CODEX / ARCHIVE
- Tab style: Cinzel, all-caps, letter-spacing, underline on active in `--gold`
- Filter rows beneath tabs use the same button style as above

---

## New Pages

If creating a new page (e.g. a character detail page, a lore article, a map page):

1. Copy the `<head>` block from index.html verbatim — it loads all fonts, sets the viewport, and links the favicon
2. Include the same `:root` CSS variable block
3. Include the `body::before` texture overlay
4. Use the same masthead HTML structure at the top
5. Wrap all content in `.page-wrap`
6. End with the same `<footer>` structure

---

## What to Avoid

- ❌ White or light backgrounds anywhere
- ❌ Sans-serif fonts (no Arial, Helvetica, system-ui, etc.)
- ❌ Bright or saturated accent colours (no blue, green, purple, orange)
- ❌ Flat or material design patterns (no shadows that look "elevated", no floating action buttons)
- ❌ Rounded pill buttons
- ❌ Animations faster than 200ms or that feel "snappy/modern"
- ❌ Emoji in UI (except the codex category filter pills which already use them)
- ❌ Any font size below 0.7rem
