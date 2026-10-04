# Banner brief for `docs/banner.jpeg`

Hand this file to the image agent as is. The result goes to `docs/banner.jpeg`
(the README embeds it at full width). It is the sibling of the `arlo-rs` banner,
`../arlo-rs/docs/banner.jpeg`: attach that image as the style reference.

---

Create a wide README banner for an open-source Rust project named
**arlo-camera-streamer**. It must look like it belongs to the same family as the
attached reference banner (the `arlo-rs` library): same art direction, same
palette, same typography, same layout grammar — but it is a different product and
must tell its own story.

## Canvas

- Exactly **4128 × 1024 pixels** (4:1), landscape, JPEG, quality 90, under 2 MB.
- Keep every element of meaning inside a safe area that leaves 4 % of the width
  and 8 % of the height empty on each side; the image is scaled to narrow screens.
- No border, no drop shadow around the canvas, no watermark, no signature.

## Style (match the reference)

- Flat vector illustration with soft inner shading, clean black outlines on the
  characters, no photo textures, no 3D render look.
- Background: deep navy, near `#1A2233`, with a faint perspective floor grid
  fading into darkness and a subtle radial glow behind the text block.
- Palette: navy background, cool grey-blue for secondary text (`#B8C4D6`), pure
  white for the title, **Rust orange `#F46623`** as the single accent (the crab,
  the accent word, cables, small highlights). A touch of electric blue for "LIVE"
  indicators and cable glow, as in the reference.
- Typography: a clean geometric sans-serif (Inter-like), heavy weight for the
  title, regular weight for the tagline and the feature lines. Text is crisp,
  rasterised at full resolution, spelled exactly as given below.

## Composition

Left 45 %: the illustration. Right 55 %: the text block, left-aligned, vertically
centred, with the same proportions as the reference.

### Illustration (left)

The story is **"the camera sleeps while the NVR still sees"**.

1. On the far left, one small **battery-powered wireless security camera**
   (generic white dome-and-body shape on a short magnetic mount, no brand mark)
   sitting on a dark ledge, **asleep**: eyes closed, three small "z z z" marks,
   a half-moon above it, a small full battery icon beside it glowing green.
2. In the middle, **Ferris the Rust crab** (the same cheerful orange crab as the
   reference, round and friendly, two claws, small shiny eyes) standing like a
   bridge: one claw gently holds an orange cable that comes from the sleeping
   camera, the other claw hands a glowing **blue cable** to a monitor on the right.
3. On the right of the illustration, a **wall-mounted NVR monitor** showing a
   2 × 2 grid of camera tiles, like a security dashboard. Three tiles show a
   calm still image of a house entrance or a garden at night with a small grey
   "IDLE" pill; the fourth tile is brighter, with a person's silhouette at a
   door and a small blue "LIVE" pill with a dot. A tiny motion-sensor pulse
   (three concentric arcs) sits above that fourth tile.
4. Under the crab, a very subtle label on the floor grid reading `RTSP · HLS`
   in small grey-blue capitals (this is the only text allowed on the left side).

The whole scene must read at a glance as: camera asleep → crab in the middle →
NVR always has a picture.

### Text block (right), exactly these strings, one per line

1. Title, white, with the last word in Rust orange:
   `arlo-camera-` in white and `streamer` in orange, written as one word on one
   line: **arlo-camera-streamer**
2. Tagline, grey-blue, lighter and about 45 % of the title's size:
   **Arlo cameras on your NVR. No battery drain.**
3. Two feature lines, grey-blue, about 28 % of the title's size, with a middle
   dot `·` as separator:
   **Idle still ↔ live splice · motion over MQTT · WebRTC & app-view relay**
   **RTSP & HLS out · daily battery budget · Frigate-ready**

No other words anywhere. No "Arlo" logo, no "Frigate" logo, no Rust logo, no
GitHub logo: brand names appear only as plain text in the lines above.

## Must not

- No real product logos or trademarked marks.
- No photographic elements, no glossy 3D, no lens flares.
- No extra decorative text, lorem ipsum, or UI chrome beyond the two pills.
- No red "REC" dots; this project reduces camera activity, it does not record
  continuously.
- Do not crop the crab or the monitor; nothing touches the canvas edge.

## Deliver

- `banner.jpeg`, 4128 × 1024, sRGB.
- A second file `banner@1x.jpeg` at 2064 × 512 for previews.
- One sentence listing the fonts used.
