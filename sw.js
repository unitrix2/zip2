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
            event.respondWith(new Response('Stream expired or Service Worker reset. Please reload the page.', { status: 404 }));
            return;
        }
        event.respondWith(handleStreamRequest(event.request, fileData, url.searchParams.get('dl') === '1'));
    }
});

async function handleStreamRequest(request, f, isDownload) {
    const rangeHeader = request.headers.get('Range');
    let start = 0;
    let end = f.size - 1;

    // Parse requested byte range from the browser's video player
    if (rangeHeader) {
        const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
        if (match) {
            start = parseInt(match[1], 10);
            if (match[2]) end = parseInt(match[2], 10);
        }
    }

    try {
        let actualStart = f.dataStart;
        let actualEnd = f.dataEnd;

        // Perfect mapping for 'Stored' media files
        if (f.compression === 0) {
            actualStart = f.dataStart + start;
            actualEnd = f.dataStart + end;
        }

        // ✨ NEW LOGIC: Auto-Reconnecting Stream Wrapper to fix 2GB cutoff bug
        const resilientStream = new ReadableStream({
            async start(controller) {
                let currentOffset = actualStart;
                let retryCount = 0;

                async function fetchNextChunk() {
                    if (currentOffset > actualEnd) {
                        controller.close();
                        return;
                    }
                    try {
                        const headers = new Headers();
                        headers.set('Range', `bytes=${currentOffset}-${actualEnd}`);
                        const res = await fetch(f.zipUrl, { headers });
                        
                        if (!res.ok) throw new Error(`HTTP Error: ${res.status}`);
                        
                        const reader = res.body.getReader();
                        retryCount = 0; // Reset retries on successful connection

                        while (true) {
                            const { done, value } = await reader.read();
                            if (done) break; // Network cut the connection or finished naturally
                            controller.enqueue(value);
                            currentOffset += value.byteLength;
                        }
                        
                        // If connection drops before finishing, fetch the remaining silently
                        if (currentOffset <= actualEnd) {
                            fetchNextChunk();
                        } else {
                            controller.close();
                        }
                    } catch (e) {
                        retryCount++;
                        if (retryCount > 5) {
                            controller.error(e);
                            return;
                        }
                        // Short delay before reconnecting on network error
                        setTimeout(fetchNextChunk, 1000);
                    }
                }
                fetchNextChunk();
            }
        });

        let finalStream = resilientStream;
        
        // Deflated files stream through decompression engine
        if (f.compression === 8) {
            finalStream = finalStream.pipeThrough(new DecompressionStream('deflate-raw'));
        }

        const responseHeaders = new Headers();
        responseHeaders.set('Access-Control-Allow-Origin', '*');

        if (isDownload) {
            responseHeaders.set('Content-Disposition', `attachment; filename="${encodeURIComponent(f.name)}"`);
            responseHeaders.set('Content-Type', 'application/octet-stream');
            responseHeaders.set('Content-Length', f.size.toString());
            return new Response(finalStream, { status: 200, headers: responseHeaders });
        } else {
            responseHeaders.set('Content-Type', getMimeType(f.name));
            
            if (f.compression === 0) {
                responseHeaders.set('Accept-Ranges', 'bytes');
                if (rangeHeader) {
                    responseHeaders.set('Content-Range', `bytes ${start}-${end}/${f.size}`);
                    responseHeaders.set('Content-Length', (end - start + 1).toString());
                    return new Response(finalStream, { status: 206, headers: responseHeaders });
                }
            } else {
                responseHeaders.set('Accept-Ranges', 'none');
            }
            
            responseHeaders.set('Content-Length', f.size.toString());
            return new Response(finalStream, { status: 200, headers: responseHeaders });
        }
    } catch(e) {
        console.error("SW Fetch Error:", e);
        return new Response(e.message, { status: 500 });
    }
}

function getMimeType(name) {
    const ext = name.split('.').pop().toLowerCase();
    const map = {
        'mp4': 'video/mp4', 'mkv': 'video/mp4', 'avi': 'video/mp4', 'webm': 'video/webm',
        'mp3': 'audio/mpeg', 'ogg': 'audio/ogg', 'wav': 'audio/wav',
        'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'png': 'image/png', 'gif': 'image/gif', 'webp': 'image/webp',
        'pdf': 'application/pdf',
        'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'xls': 'application/vnd.ms-excel',
        'xlsm': 'application/vnd.ms-excel.sheet.macroEnabled.12',
        'csv': 'text/csv',
        'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
        'doc': 'application/msword',
        'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
        'ppt': 'application/vnd.ms-powerpoint'
    };
    return map[ext] || 'application/octet-stream';
}
