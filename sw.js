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
        event.respondWith(handleStreamRequest(event.request, fileData, url.searchParams.get('dl') === '1'));
    }
});

async function handleStreamRequest(request, f, isDownload) {
    try {
        const rangeHeader = request.headers.get('Range');
        
        // --- NATIVE PROXY ENGINE WITH SMART HEADER SPOOFING ---
        if (f.compression === 0) {
            let start = 0;
            let end = f.size - 1;

            // Browser video player sends exact bytes it needs
            if (rangeHeader) {
                const match = rangeHeader.match(/bytes=(\d+)-(\d*)/);
                if (match) {
                    start = parseInt(match[1], 10);
                    if (match[2]) end = parseInt(match[2], 10);
                }
            }

            // Boundary checks to prevent out-of-range crashes
            if (start >= f.size) start = f.size - 1;
            if (end >= f.size) end = f.size - 1;

            // Map local video bytes to global ZIP coordinates
            const reqStart = f.dataStart + start;
            const reqEnd = f.dataStart + end;
            
            const fetchHeaders = new Headers();
            fetchHeaders.set('Range', `bytes=${reqStart}-${reqEnd}`);
            
            // Direct proxy fetch
            const res = await fetch(f.zipUrl, { headers: fetchHeaders });
            
            if (!res.ok) throw new Error("Server rejected proxy request.");

            // ✨ CRITICAL FIX: We MUST rewrite the headers here. 
            // Do NOT forward the original Cloudflare headers, they confuse the Mobile browser's audio multiplexer.
            const resHeaders = new Headers();
            resHeaders.set('Access-Control-Allow-Origin', '*');
            
            if (isDownload) {
                resHeaders.set('Content-Disposition', `attachment; filename="${encodeURIComponent(f.name)}"`);
                resHeaders.set('Content-Type', 'application/octet-stream');
                resHeaders.set('Content-Length', f.size.toString());
                return new Response(res.body, { status: 200, headers: resHeaders });
            } else {
                resHeaders.set('Content-Type', getMimeType(f.name));
                resHeaders.set('Accept-Ranges', 'bytes');
                
                // Construct completely fake "Content-Range" so the browser thinks it's a standalone video file
                if (rangeHeader) {
                    resHeaders.set('Content-Range', `bytes ${start}-${end}/${f.size}`);
                    resHeaders.set('Content-Length', (end - start + 1).toString());
                    return new Response(res.body, { status: 206, headers: resHeaders });
                }
                
                resHeaders.set('Content-Length', f.size.toString());
                return new Response(res.body, { status: 200, headers: resHeaders });
            }
        } 
        else {
            // Deflated files
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
                resHeaders.set('Content-Type', getMimeType(f.name));
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
