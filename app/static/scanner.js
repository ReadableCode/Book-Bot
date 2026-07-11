/* Barcode scanning: native BarcodeDetector where available (Chrome/Android),
   vendored ZXing everywhere else (iOS Safari). The camera stream is managed
   here; detection can be paused (while the result sheet is open) without
   dropping the stream.

   The ZXing path runs its own frame loop — grab a video frame onto a canvas,
   decode that bitmap — instead of ZXing's decodeFromStream, whose video
   attach/play plumbing proved unreliable (decode loop intermittently never
   fires despite decodable frames). Direct per-frame decoding is deterministic. */

const Scanner = (() => {
  let stream = null;
  let video = null;
  let onCode = null;
  let running = false;   // camera on
  let detecting = false; // actively looking for barcodes
  let detector = null;
  let timerId = null;
  let zxingReader = null;
  let zxingHints = null;
  let grabCanvas = null;

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
    timerId = setTimeout(nativeLoop, 180);
  }

  function zxingLoop() {
    if (!running) return;
    if (detecting && video.readyState >= 2 && video.videoWidth) {
      if (grabCanvas.width !== video.videoWidth || grabCanvas.height !== video.videoHeight) {
        grabCanvas.width = video.videoWidth;
        grabCanvas.height = video.videoHeight;
      }
      grabCanvas.getContext("2d").drawImage(video, 0, 0);
      try {
        const bitmap = new ZXing.BinaryBitmap(
          new ZXing.HybridBinarizer(new ZXing.HTMLCanvasElementLuminanceSource(grabCanvas))
        );
        const result = zxingReader.decode(bitmap, zxingHints);
        if (result) handleCode(result.getText());
      } catch { /* no barcode in this frame — keep looping */ }
    }
    timerId = setTimeout(zxingLoop, 200);
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
      detector = new window.BarcodeDetector({ formats: ["ean_13", "upc_a", "ean_8"] });
      nativeLoop();
    } else {
      zxingReader = new ZXing.MultiFormatReader();
      zxingHints = new Map();
      zxingHints.set(ZXing.DecodeHintType.POSSIBLE_FORMATS, [
        ZXing.BarcodeFormat.EAN_13,
        ZXing.BarcodeFormat.UPC_A,
        ZXing.BarcodeFormat.EAN_8,
      ]);
      zxingHints.set(ZXing.DecodeHintType.TRY_HARDER, true);
      grabCanvas = document.createElement("canvas");
      zxingLoop();
    }
  }

  function pause() { detecting = false; }
  function resume() { if (running) detecting = true; }

  function stop() {
    running = false;
    detecting = false;
    if (timerId) { clearTimeout(timerId); timerId = null; }
    if (stream) {
      stream.getTracks().forEach((t) => t.stop());
      stream = null;
    }
    if (video) video.srcObject = null;
  }

  return { start, stop, pause, resume, isRunning: () => running };
})();
