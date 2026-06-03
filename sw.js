self.addEventListener('install', event => {
    self.skipWaiting();
});

self.addEventListener('activate', event => {
    event.waitUntil(clients.claim());
});

const activeStreams = new Map();

self.addEventListener('message', event => {
    if (event.data.type === 'REGISTER_STREAM') {
        activeStreams.set(event.data.id, event.data.payload);
        if (event.ports && event.ports[0]) {
            event.ports[0].postMessage({ status: 'ok' });
        }
    }
});

self.addEventListener('fetch', event => {
    const url = new URL(event.request.url);
    
    if (url.searchParams.has('sw_stream')) {
        const id = url.searchParams.get('sw_stream');
        const fileData = activeStreams.get(id);

        if (!fileData) {
            event.respondWith(new Response('Stream expired. Please reload.', { status: 404 }));
            return;
        }
        event.respondWith(handleStreamRequest(event.request, fileData, url));
    }
});

async function handleStreamRequest(request, f, urlObj) {
    try {
        const rangeHeader = request.headers.get('Range');
        const isDownload = urlObj.searchParams.get('dl') === '1';
        const isDataSaver = urlObj.searchParams.get('saver') === '1';
        const mime = getMimeType(f.name);

        // ==========================================
        // ENGINE 1: DOWNLOADER (STRICTLY UNTOUCHED)
        // ==========================================
        if (isDownload) {
            const resilientStream = new ReadableStream({
                async start(controller) {
                    let currentOffset = f.dataStart;
                    const actualEnd = f.dataEnd;
                    let retryCount = 0;

                    async function fetchNextChunk() {
                        if (currentOffset > actualEnd) {
                            try { controller.close(); } catch(e){}
                            return;
                        }
                        try {
                            const headers = new Headers();
                            headers.set('Range', `bytes=${currentOffset}-${actualEnd}`);
                            const res = await fetch(f.zipUrl, { headers });
                            
                            if (!res.ok) throw new Error("HTTP Error");
                            const reader = res.body.getReader();
                            retryCount = 0;

                            while (true) {
                                const { done, value } = await reader.read();
                                if (done) break;
                                controller.enqueue(value);
                                currentOffset += value.byteLength;
                            }
                            if (currentOffset <= actualEnd) {
                                fetchNextChunk();
                            } else {
                                try { controller.close(); } catch(e){}
                            }
                        } catch (e) {
                            retryCount++;
                            if (retryCount > 10) { try { controller.error(e); } catch(err){} return; }
                            setTimeout(fetchNextChunk, 1000);
                        }
                    }
                    fetchNextChunk();
                }
            });

            let finalStream = f.compression === 8 ? resilientStream.pipeThrough(new DecompressionStream('deflate-raw')) : resilientStream;

            const resHeaders = new Headers();
            resHeaders.set('Access-Control-Allow-Origin', '*');
            resHeaders.set('Content-Disposition', `attachment; filename="${encodeURIComponent(f.name)}"`);
            resHeaders.set('Content-Type', 'application/octet-stream');
            resHeaders.set('Content-Length', f.size.toString());
            return new Response(finalStream, { status: 200, headers: resHeaders });
        }

        // ==========================================
        // ENGINE 2: PLAYBACK (FAST MODE & DATA SAVER)
        // ==========================================
        if (f.compression === 0) {
            let start = 0;
            let end = f.size - 1;

            if (rangeHeader) {
                const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
                if (match) {
                    start = parseInt(match[1], 10);
                    if (match[2]) end = parseInt(match[2], 10);
                }
            }

            if (start >= f.size) start = f.size - 1;
            if (end >= f.size) end = f.size - 1;

            const reqStart = f.dataStart + start;
            const reqEnd = f.dataStart + end;
            
            const fetchHeaders = new Headers();
            fetchHeaders.set('Range', `bytes=${reqStart}-${reqEnd}`);
            
            // ⚡ FAST MODE (Default): Pure Native Proxy. Solves the 908.7 KB jump deadlock.
            if (!isDataSaver) {
                const res = await fetch(f.zipUrl, { headers: fetchHeaders });
                if (!res.ok) throw new Error("Proxy error.");

                const resHeaders = new Headers();
                resHeaders.set('Access-Control-Allow-Origin', '*');
                resHeaders.set('Content-Type', mime);
                resHeaders.set('Accept-Ranges', 'bytes');
                
                if (rangeHeader) {
                    resHeaders.set('Content-Range', `bytes ${start}-${end}/${f.size}`);
                    resHeaders.set('Content-Length', (end - start + 1).toString());
                    return new Response(res.body, { status: 206, headers: resHeaders });
                }
                resHeaders.set('Content-Length', f.size.toString());
                return new Response(res.body, { status: 200, headers: resHeaders });
            } 
            // 🐢 DATA SAVER MODE: Chunks the stream to save background data
            else {
                const abortCtrl = new AbortController();
                request.signal.addEventListener('abort', () => abortCtrl.abort());
                const res = await fetch(f.zipUrl, { headers: fetchHeaders, signal: abortCtrl.signal });
                
                const stream = new ReadableStream({
                    async start(controller) {
                        const reader = res.body.getReader();
                        let totalRead = 0;
                        const BURST = 5 * 1024 * 1024; // 5MB fast burst

                        async function pump() {
                            try {
                                const { done, value } = await reader.read();
                                if (done) { controller.close(); return; }
                                
                                controller.enqueue(value);
                                totalRead += value.byteLength;
                                
                                if (totalRead > BURST) {
                                    // Throttle download to save data
                                    await new Promise(r => setTimeout(r, 150));
                                }
                                pump();
                            } catch (err) {
                                controller.error(err);
                            }
                        }
                        pump();
                    },
                    cancel() { abortCtrl.abort(); }
                });

                const resHeaders = new Headers();
                resHeaders.set('Access-Control-Allow-Origin', '*');
                resHeaders.set('Content-Type', mime);
                resHeaders.set('Accept-Ranges', 'bytes');
                if (rangeHeader) {
                    resHeaders.set('Content-Range', `bytes ${start}-${end}/${f.size}`);
                    resHeaders.set('Content-Length', (end - start + 1).toString());
                    return new Response(stream, { status: 206, headers: resHeaders });
                }
                resHeaders.set('Content-Length', f.size.toString());
                return new Response(stream, { status: 200, headers: resHeaders });
            }
        } 
        else {
            const fetchHeaders = new Headers();
            fetchHeaders.set('Range', `bytes=${f.dataStart}-${f.dataEnd}`);
            
            const res = await fetch(f.zipUrl, { headers: fetchHeaders });
            const stream = res.body.pipeThrough(new DecompressionStream('deflate-raw'));
            
            const resHeaders = new Headers();
            resHeaders.set('Access-Control-Allow-Origin', '*');
            resHeaders.set('Content-Type', mime);
            resHeaders.set('Accept-Ranges', 'none'); 
            return new Response(stream, { status: 200, headers: resHeaders });
        }
    } catch(e) {
        console.error("SW Proxy Error:", e);
        return new Response(e.message, { status: 500 });
    }
}

function getMimeType(name) {
    const ext = name.split('.').pop().toLowerCase();
    const map = {
        'mp4': 'video/mp4', 'mkv': 'video/x-matroska', 'avi': 'video/x-msvideo', 'webm': 'video/webm',
        'mp3': 'audio/mpeg', 'ogg': 'audio/ogg', 'wav': 'audio/wav',
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif',
        'pdf': 'application/pdf', 'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
    };
    return map[ext] || 'application/octet-stream';
}
