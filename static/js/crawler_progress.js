(() => {
  const dataScript = document.getElementById("crawler-progress-config");
  if (!dataScript) {
    return;
  }

  let config = {};
  try {
    config = JSON.parse(dataScript.textContent || "{}");
  } catch (error) {
    console.error("Failed to parse crawl progress config:", error);
    return;
  }

  const progressFill = document.getElementById("progress-fill");
  const progressStatus = document.getElementById("progress-status");
  const processingFill = document.getElementById("processing-fill");
  const processingStatus = document.getElementById("processing-status");
  if (!progressFill || !progressStatus) {
    return;
  }

  if (!window.EventSource) {
    progressStatus.textContent = "Live progress not supported in this browser.";
    return;
  }

  const jobId = config.jobId;
  if (!jobId || !config.progressUrl) {
    return;
  }

  const limit = Number(config.limit) || 1;
  const source = new EventSource(config.progressUrl);

  const closeAndReload = (delayMs) => {
    source.close();
    window.setTimeout(() => {
      window.location.reload();
    }, delayMs);
  };

  source.onmessage = (event) => {
    if (!event.data) {
      return;
    }

    let payload;
    try {
      payload = JSON.parse(event.data);
    } catch (error) {
      console.warn("Malformed crawl progress message:", error);
      return;
    }

    if (payload.type === "progress") {
      const limitValue = Number(payload.limit) || limit;
      const percent = Math.min(100, Math.round((payload.visited / limitValue) * 100));
      progressFill.classList.remove("complete");
      progressFill.style.width = `${percent}%`;
      const statusText = payload.status ? ` — ${payload.status}` : "";
      progressStatus.textContent = `Visited ${payload.visited} of ${limitValue} pages${statusText}`;
      if (processingFill && processingStatus) {
        processingFill.style.width = "10%";
        processingStatus.textContent = "Preparing entity extraction…";
      }
    } else if (payload.type === "processing") {
      if (processingFill && processingStatus) {
        const processed = Number(payload.processed || 0);
        const total = Number(payload.total || 0);
        if (payload.stage === "start") {
          processingFill.classList.remove("complete");
          const pct = total > 0 ? Math.max(5, Math.round((processed / total) * 100)) : 5;
          processingFill.style.width = `${pct}%`;
          processingStatus.textContent = total > 0
            ? `Processing ${processed}/${total} pages… (${pct}%)`
            : "Consolidating literal and heuristic matches…";
        } else if (payload.stage === "progress") {
          const pct = total > 0 ? Math.min(99, Math.round((processed / total) * 100)) : 50;
          processingFill.classList.remove("complete");
          processingFill.style.width = `${pct}%`;
          processingStatus.textContent = total > 0
            ? `Processing ${processed}/${total} pages… (${pct}%)`
            : "Processing…";
        } else if (payload.stage === "done") {
          processingFill.classList.add("complete");
          processingFill.style.width = "100%";
          processingStatus.textContent = "Entity extraction complete. Rendering results… (100%)";
        } else if (payload.stage === "error") {
          processingFill.classList.remove("complete");
          processingFill.style.width = "100%";
          processingStatus.textContent = `Processing error: ${payload.message || "Unknown issue"}`;
        }
      }
    } else if (payload.type === "error") {
      progressStatus.textContent = `Error: ${payload.message || "Unknown error"}`;
      closeAndReload(2000);
    } else if (payload.type === "complete") {
      progressFill.classList.add("complete");
      progressFill.style.width = "100%";
      progressStatus.textContent = "Crawl complete. Loading results…";
      if (processingFill && processingStatus) {
        processingFill.classList.add("complete");
        processingFill.style.width = "100%";
        processingStatus.textContent = "Entity extraction complete. Loading results…";
      }
      closeAndReload(1200);
    }
  };

  source.onerror = () => {
    progressStatus.textContent = "Connection lost. Waiting for final results…";
    closeAndReload(2000);
  };
})();

