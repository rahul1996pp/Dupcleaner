package dup

import (
	"bytes"
	"image"
	"image/color"
	"image/jpeg"
	"testing"
)

// makeBenchJPEG builds a realistic ~12 MP photo-like JPEG once, so decode cost
// is representative of phone/camera images (not a trivially-compressible solid).
func makeBenchJPEG(b *testing.B) []byte {
	const w, h = 4096, 3072
	src := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			// busy gradient + checker so the encoder keeps real high-freq detail
			r := uint8((x*7 + y*3) % 256)
			g := uint8((x ^ y) % 256)
			bl := uint8((x*y/64 + ((x/16+y/16)&1)*53) % 256)
			src.Set(x, y, color.RGBA{r, g, bl, 255})
		}
	}
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, src, &jpeg.Options{Quality: 85}); err != nil {
		b.Fatal(err)
	}
	return buf.Bytes()
}

func BenchmarkDecodeOnly(b *testing.B) {
	data := makeBenchJPEG(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		_ = img
	}
}

func BenchmarkDecodePlusToNRGBA(b *testing.B) {
	data := makeBenchJPEG(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		_ = toNRGBA(img)
	}
}

// BenchmarkHashPath measures the full per-image cost as the scanner runs it:
// decode → 3 perceptual hashes. With the Y-plane sampler there is no NRGBA
// conversion; the hashes read luma straight from the decoded image.
func BenchmarkHashPath(b *testing.B) {
	data := makeBenchJPEG(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		img, _, err := image.Decode(bytes.NewReader(data))
		if err != nil {
			b.Fatal(err)
		}
		_ = dHashFast(img)
		_ = aHashFast(img)
		_ = pHashFast(img)
	}
}
