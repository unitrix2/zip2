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
        const isAudioFallback = urlObj.searchParams.get('audio_fallback') === '1';
        
        let mime = getMimeType(f.name);
        if (isAudioFallback && mime.startsWith('video/')) {
            mime = mime.replace('video/', 'audio/');
        }

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
            
            const res = await fetch(f.zipUrl, { headers: fetchHeaders });
            if (!res.ok) throw new Error("Server rejected proxy request.");

            // ✨ TURBO PIPELINE: Wrap stream to aggressively pull chunks and calculate live telemetry
            const reader = res.body.getReader();
            const telemetryChannel = new BroadcastChannel('sw-telemetry');
            
            const stream = new ReadableStream({
                async start(controller) {
                    let loadedSinceLast = 0;
                    let lastTime = Date.now();
                    let totalLoaded = 0;
                    const totalBytes = reqEnd - reqStart + 1;

                    try {
                        while (true) {
                            const { done, value } = await reader.read();
                            if (done) {
                                controller.close();
                                break;
                            }
                            controller.enqueue(value);
                            
                            loadedSinceLast += value.byteLength;
                            totalLoaded += value.byteLength;
                            
                            const now = Date.now();
                            if (now - lastTime > 400) {
                                const speedMBps = (loadedSinceLast / (1024 * 1024)) / ((now - lastTime) / 1000);
                                telemetryChannel.postMessage({
                                    type: 'PROGRESS',
                                    speed: speedMBps.toFixed(2),
                                    loaded: totalLoaded,
                                    total: totalBytes
                                });
                                lastTime = now;
                                loadedSinceLast = 0;
                            }
                        }
                    } catch (err) {
                        controller.error(err);
                    }
                },
                cancel(reason) {
                    reader.cancel(reason); // Fixes Deadlocks if browser skips ahead
                }
            });

            const resHeaders = new Headers();
            resHeaders.set('Access-Control-Allow-Origin', '*');
            
            if (isDownload) {
                resHeaders.set('Content-Disposition', `attachment; filename="${encodeURIComponent(f.name)}"`);
                resHeaders.set('Content-Type', 'application/octet-stream');
                resHeaders.set('Content-Length', f.size.toString());
                return new Response(stream, { status: 200, headers: resHeaders });
            } else {
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
            
            if (isDownload) {
                resHeaders.set('Content-Disposition', `attachment; filename="${encodeURIComponent(f.name)}"`);
                resHeaders.set('Content-Type', 'application/octet-stream');
            } else {
                resHeaders.set('Content-Type', mime);
                resHeaders.set('Accept-Ranges', 'none'); 
            }
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
