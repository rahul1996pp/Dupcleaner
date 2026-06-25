package dup

import (
	"image"
	"image/color"
	"testing"
)

// expLuma is the integer BT.601 luma the sampler should yield for an opaque
// (R,G,B): the same 299/587/114 ÷1000 weighting used throughout.
func expLuma(r, g, b uint32) uint32 { return (299*r + 587*g + 114*b) / 1000 }

// TestLumaSamplerTypes pins lumaSamplerFor across every concrete image type the
// decoders produce, including the premultiplied-alpha un-premultiply for RGBA.
func TestLumaSamplerTypes(t *testing.T) {
	const R, G, B = 200, 150, 90
	want := expLuma(R, G, B)

	// NRGBA (PNG / x/image decoders) — straight alpha.
	nr := image.NewNRGBA(image.Rect(0, 0, 2, 2))
	nr.SetNRGBA(1, 1, color.NRGBA{R, G, B, 255})
	if s, _, _ := lumaSamplerFor(nr); s(1, 1) != want {
		t.Errorf("NRGBA: got %d want %d", s(1, 1), want)
	}

	// RGBA opaque — premultiplied == straight when a==255.
	ra := image.NewRGBA(image.Rect(0, 0, 2, 2))
	ra.SetRGBA(1, 1, color.RGBA{R, G, B, 255})
	if s, _, _ := lumaSamplerFor(ra); s(1, 1) != want {
		t.Errorf("RGBA opaque: got %d want %d", s(1, 1), want)
	}

	// RGBA semi-transparent — Pix holds premultiplied bytes; the sampler must
	// un-premultiply so luma matches the straight-alpha colour (R,G,B).
	ra2 := image.NewRGBA(image.Rect(0, 0, 2, 2))
	ra2.SetRGBA(1, 1, color.RGBA{R * 128 / 255, G * 128 / 255, B * 128 / 255, 128}) // premultiplied input
	got, _, _ := lumaSamplerFor(ra2)
	// Allow ±2 for the round-trip rounding through premultiplication.
	if d := int(got(1, 1)) - int(want); d < -2 || d > 2 {
		t.Errorf("RGBA semi-transparent un-premultiply: got %d want ~%d", got(1, 1), want)
	}

	// Gray — Pix value is luma directly.
	gr := image.NewGray(image.Rect(0, 0, 2, 2))
	gr.SetGray(1, 1, color.Gray{123})
	if s, _, _ := lumaSamplerFor(gr); s(1, 1) != 123 {
		t.Errorf("Gray: got %d want 123", s(1, 1))
	}

	// YCbCr (JPEG) — Y plane is luma; the sampler reads it directly.
	yc := image.NewYCbCr(image.Rect(0, 0, 2, 2), image.YCbCrSubsampleRatio444)
	yc.Y[yc.YOffset(1, 1)] = 77
	if s, _, _ := lumaSamplerFor(yc); s(1, 1) != 77 {
		t.Errorf("YCbCr: got %d want 77", s(1, 1))
	}
}

// TestLumaSamplerNonZeroBounds verifies coordinate math when the image's bounds
// do not start at (0,0) — the sampler takes 0-based coords and must add Min.
func TestLumaSamplerNonZeroBounds(t *testing.T) {
	sub := image.NewGray(image.Rect(5, 7, 9, 11)) // 4x4 starting at (5,7)
	sub.SetGray(5, 7, color.Gray{42})             // top-left of the sub-image
	s, w, h := lumaSamplerFor(sub)
	if w != 4 || h != 4 {
		t.Fatalf("dims: got %dx%d want 4x4", w, h)
	}
	if s(0, 0) != 42 { // 0-based (0,0) must map to absolute (5,7)
		t.Errorf("non-zero bounds: got %d want 42", s(0, 0))
	}
}

// TestHashStableOnSameContent sanity-checks that the same picture encoded into
// two different concrete types yields identical perceptual hashes (so a duplicate
// is still a duplicate regardless of decode type).
func TestHashStableOnSameContent(t *testing.T) {
	const w, h = 64, 64
	mk := func() (*image.NRGBA, *image.RGBA) {
		n := image.NewNRGBA(image.Rect(0, 0, w, h))
		r := image.NewRGBA(image.Rect(0, 0, w, h))
		for y := 0; y < h; y++ {
			for x := 0; x < w; x++ {
				c := color.NRGBA{uint8(x * 4), uint8(y * 4), uint8((x + y) * 2), 255}
				n.SetNRGBA(x, y, c)
				r.SetRGBA(x, y, color.RGBA{c.R, c.G, c.B, 255})
			}
		}
		return n, r
	}
	n, r := mk()
	if dHashFast(n) != dHashFast(r) || aHashFast(n) != aHashFast(r) || pHashFast(n) != pHashFast(r) {
		t.Error("identical opaque content must hash the same across NRGBA and RGBA")
	}
}
