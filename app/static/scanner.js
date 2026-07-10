/* Barcode scanning: native BarcodeDetector where available (Chrome/Android),
   vendored ZXing everywhere else (iOS Safari). The camera stream is managed
   here; detection can be paused (while the result sheet is open) without
   dropping the stream. */

const Scanner = (() => {
  let stream = null;
  let video = null;
  let onCode = null;
  let running = false;   // camera on
  let detecting = false; // actively looking for barcodes
  let usingZXing = false;
  let zxingReader = null;
  let detector = null;
  let rafId = null;

  async function nativeSupported() {
    if (!("BarcodeDetector" in window)) return false;
    try {
      const formats = await window.BarcodeDetector.getSupportedFormats();
      return formats.includes("ean_13");
    } catch {
      return false;
    }
  }

  async function nativeLoop() {
    if (!running) return;
    if (detecting && video.readyState >= 2) {
      try {
        const codes = await detector.detect(video);
        if (codes.length && detecting) {
          handleCode(codes[0].rawValue);
        }
      } catch { /* detection hiccup — keep looping */ }
    }
    rafId = setTimeout(nativeLoop, 180);
  }

  function handleCode(text) {
    if (!detecting) return;
    detecting = false;
    if (navigator.vibrate) navigator.vibrate(80);
    onCode(text);
  }

  async function start(videoEl, codeCallback) {
    video = videoEl;
    onCode = codeCallback;
    stream = await navigator.mediaDevices.getUserMedia({
      audio: false,
      video: {
        facingMode: { ideal: "environment" },
        width: { ideal: 1280 },
        height: { ideal: 720 },
      },
    });
    video.srcObject = stream;
    await video.play();
    running = true;
    detecting = true;

    if (await nativeSupported()) {
      usingZXing = false;
      detector = new window.BarcodeDetector({ formats: ["ean_13", "upc_a", "ean_8"] });
      nativeLoop();
    } else {
      usingZXing = true;
      const hints = new Map();
      hints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
        ZXing.BarcodeFormat.EAN_13,
        ZXing.BarcodeFormat.UPC_A,
        ZXing.BarcodeFormat.EAN_8,
      ]);
      hints.set(ZXing.DecodeHintType.TRY_HARDER, true);
      zxingReader = new ZXing.BrowserMultiFormatReader(hints);
      zxingReader.decodeFromStream(stream, video, (result) => {
        if (result) handleCode(result.getText());
      });
    }
  }

  function pause() { detecting = false; }
  function resume() { if (running) detecting = true; }

  function stop() {
    running = false;
    detecting = false;
    if (rafId) { clearTimeout(rafId); rafId = null; }
    if (zxingReader) {
      try { zxingReader.reset(); } catch { /* already stopped */ }
      zxingReader = null;
    }
    if (stream) {
      stream.getTracks().forEach((t) => t.stop());
      stream = null;
    }
    if (video) video.srcObject = null;
  }

  return { start, stop, pause, resume, isRunning: () => running };
})();
