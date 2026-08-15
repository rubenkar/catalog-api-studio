# Bbox Detection Testbench — User Guide

## Overview

The Bbox Detection Testbench is a debug dialog for testing and comparing object detection algorithms. It supports 12 different detection methods, frame-by-frame animation, and two visualization modes: standard (with bounding box overlays) and mask (showing detected vs. unprocessed areas).

**Launch**: Double-click any page in the Preview tab to open the testbench.

---

## UI Controls

### Top Bar

- **Algorithm**: Dropdown to select detection method (0-11)
- **Merge rows** (checkbox): Post-process small bboxes into text lines
- **Bboxes** (checkbox): Show bounding box borders and colors
- **Mask** (checkbox): Switch between standard and mask visualization modes

### Animation Controls

- **▶ Play steps** button: Cycle through detection steps at 100ms per frame
- **Animation slider**: Scrub manually through steps; shows pass boundary markers
- **Step counter**: "Step N/Total" display

### Information Bar

- Page dimensions (pixels × DPI)
- Object count and detection time (ms)
- Current algorithm and parameters

---

## Detection Methods

### Methods 0-8: Scanline Variants
- **v1 Original**: Row-first scanning, 1px grow steps, O(n²) skip tracking
- **v2 Visited Fast**: Visited mask, fast grow (jump to content)
- **v3 Adaptive**: Dynamic margin based on object size
- **OpenCV CCA**: Connected Component Analysis
- **OpenCV CCA + dilation**: CCA with dilation preprocessing
- **MSER**: Maximally Stable Extremal Regions (text zones)
- **Scanline + MSER**: Hybrid scanline and MSER
- **PDF text + Scanline**: Extract PDF text blocks, scanline for rest

### Method 8: Hybrid 2-pass
- **Pass 1**: Scanline with refined text masks (skip text areas)
- **Pass 2**: Full binary merge (find larger structures containing Pass 1 objects)
- **Merge logic**: Pass 2 bbox containing 2+ Pass 1 bboxes → table

### Method 9: PDF Objects Only
- Extract native PDF objects: text blocks, images, tables, graphics
- No raster processing; pure PDF structure detection
- Fast, complementary to other methods

### Method 10: Pure PDF
- Reserved for future pure PDF extraction

### Method 11: Hybrid 2-pass V.2 ⭐
Advanced multi-stage detection with animation:

1. **Pre-Pass**: Per-PDF-text-object processing
   - Isolate each text block (skip other PDF objects)
   - Run column-first scanline+grow inside bounds only
   - Render: page image with text blocks visible, rest white
   - Marker: Gray label "Pre-Pass" on slider

2. **Pass 1**: Refined text masks
   - Scanline on full page with text-sensitive skip rects
   - Accumulates Pre-Pass detections
   - Marker: Blue label "Pass 1" on slider

3. **Pass 2**: Full-page merge
   - Scanline on full binary (no text masking)
   - Merge logic: large objects containing 2+ Pass 1 results → tables
   - Marker: Orange label "Pass 2" on slider

4. **Final**: Combined results
   - Marker: Green label "Final" on slider

---

## Visualization Modes

### Standard Mode (Mask checkbox OFF)
Shows page image with bounding box borders:
- **Blue borders**: PDF text blocks
- **Green borders**: Detected non-text objects
- **Red borders**: Non-text objects at higher hierarchies
- **Gray borders**: Small artifacts/noise

### Mask Mode (Mask checkbox ON)

#### Pre-Pass Stage (Hybrid 2-pass V.2)
- **White background**: Unprocessed areas
- **Black fill**: Text block areas (always masked)
- **Scanline indicator**: 
  - Red horizontal line (row scan) outside text
  - White horizontal line inside text
  - Cyan vertical line (column scan) outside text
  - White vertical line inside text
- **Border colors**:
  - White: PDF text block borders
  - Magenta: Growing objects >50% inside text areas
  - Green: Growing objects outside text areas

#### Pass 1+ Stages
- **Page image** (30% opacity) + **mask layer** (70% opacity)
- **Black areas**: Detected objects
- **White areas**: Unprocessed regions
- **Scanline indicator**: Red (row scan) or cyan (column scan)
- **Border colors**: Same as above

---

## Animation Playback

### Play Steps
Press **▶ Play steps** to cycle through detection frames:
- Each frame shows intermediate seed/grow operations
- Progresses from Pre-Pass → Pass 1 → Pass 2 → Final
- Automatically loops

### Manual Navigation
Click or drag the **animation slider** to scrub:
- Shows pass boundary markers with labels
- Displays current step number
- Supports rewinding and frame-by-frame inspection

### Markers on Slider
Visual indicators divide the animation:
- **Gray** (0): Pre-Pass initial state
- **Blue**: Pass 1 start
- **Orange**: Pass 2 start
- **Green**: Final result

---

## Hybrid 2-pass V.2 Workflow

### Stage 1: Pre-Pass (Column-First Scanning)
For each PDF text block:
1. Extract binary region of text block only
2. Skip all other PDF text blocks
3. Run column-first scanline (scan left-to-right, within-column)
4. Track seed and grow steps for animation
5. Accumulate detected objects

**Indicator**: Cyan vertical lines show column progress; horizontal red lines show row navigation within columns.

### Stage 2: Pass 1 (Text-Aware)
- Scan full page with text-sensitive skip rectangles
- Do NOT detect inside refined text block boundaries
- Track all seed/grow steps
- Filter out objects ≥80% covered by text

### Stage 3: Pass 2 (Merge Fragmentation)
- Scan full binary (no text masking)
- Find large objects (fragments merged from Pass 1)
- Merge logic: if Pass 2 bbox contains 2+ Pass 1 bboxes → table

### Final Output
Combine: PDF text blocks + Pre-Pass objects + Pass 1 objects + Pass 2 merged objects

---

## Tips & Troubleshooting

### Animation Not Playing?
- Ensure detection steps were generated: run detection method first
- Check "Bboxes" is enabled to see borders during animation
- Verify frame rate isn't stuck: slider should advance

### Too Many Steps?
- Pre-Pass with many text blocks generates many animation frames
- Slider speed: 100ms per frame (10 frames/second)
- Manual slider drag is faster for navigation

### Mask Mode Rendering Issues?
- Mask mode works best with **Hybrid 2-pass V.2**
- Other methods show: white = unprocessed, black = detected
- Use "Bboxes" checkbox to see borders alongside mask

### Comparing Methods?
- Select method, click slider to final frame
- Visually compare object count and placement
- Switch methods to A/B test on same page

---

## Keyboard Shortcuts

- Double-click page: Open testbench for that page
- Ctrl+R: Reload and re-detect current method
- Space: Play/pause animation (if focused on slider)

---

## Known Limitations

- **Testbench not modal**: Other UI interactions continue while open
- **Memory**: Large PDFs may load slowly during animation generation
- **Performance**: Column-first scanning (Pre-Pass V.2) is slower than row-first
- **Skip logic**: Overlapping skip rectangles may merge objects unintentionally

---

## Architecture Notes

### Core Detection Function
`_scanline_v1_core(binary, h, w, margin=3, min_obj=20, skip_rects=None, steps_out=None, col_first=False)`

- Shared core for all scanline-based methods (v1, v2, v3, Hybrid variants)
- `skip_rects`: Areas to skip (refined text masks, other PDF objects)
- `steps_out`: Animation step tracking (each seed, grow, finalize)
- `col_first`: Column-first vs. row-first scanning mode

### Rendering Pipeline
1. **_render_frame()**: Blend mask (70%) + page image (30%), draw scanline indicator
2. **_on_anim_step()**: Retrieve step from `_detection_steps`, call _render_frame()
3. **_on_anim_tick()**: Advance slider, trigger _on_anim_step()

### Pass Markers
Dynamic slider overlay showing:
- Algorithm name + color
- Step index position
- Smooth rendering even with hundreds of steps

---

## Quick Start Example

1. Open testbench (double-click page)
2. Select **Hybrid 2-pass V.2** from Algorithm dropdown
3. Enable **Mask** checkbox
4. Click **▶ Play steps**
5. Watch animation progress through Pre-Pass → Pass 1 → Pass 2
6. Use slider to rewind and inspect specific stages
7. Toggle **Bboxes** to show/hide object borders

---

**Last Updated**: 2026-04-12  
**Status**: Active & Maintained
