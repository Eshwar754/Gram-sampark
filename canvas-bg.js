// Optimized Antigravity Interactive Canvas Background
const canvas = document.createElement('canvas');
canvas.id = 'ag-canvas';
canvas.style.position = 'fixed';
canvas.style.top = '0';
canvas.style.left = '0';
canvas.style.pointerEvents = 'none';
canvas.style.zIndex = '-1';
document.body.prepend(canvas);

const ctx = canvas.getContext('2d', { alpha: true });

let width, height, dpr;
let particles = [];
let isTabActive = true;
let animationFrameId;

// Throttle cursor updates via lerp targets
let mouseX = -1000, mouseY = -1000;
let targetMouseX = -1000, targetMouseY = -1000;
let mouseActive = false;
let isMobile = false;

document.addEventListener('visibilitychange', () => {
    isTabActive = !document.hidden;
    if (isTabActive) {
        animate(performance.now());
    } else {
        cancelAnimationFrame(animationFrameId);
    }
});

let config = {
    maxParticles: 100, // Hard limit for smooth 60fps on all devices
    connectionDistance: 140,
    baseSpeed: 0.25,
    // Pre-calculated RGB strings to avoid hex conversion on every frame
    rgbColors: [
        '20, 184, 166', // 14B8A6 (Teal)
        '15, 118, 110', // 0F766E (Dark Teal)
        '14, 165, 233', // 0ea5e9 (Cyan)
        '16, 185, 129', // 10b981 (Emerald)
        '245, 158, 11', // f59e0b (Gold)
    ]
};

function resize() {
    isMobile = window.innerWidth < 768;
    
    // Cap devicePixelRatio max to 1.5 for performance to prevent lag on retina displays
    dpr = Math.min(window.devicePixelRatio || 1, 1.5); 
    
    width = window.innerWidth;
    height = window.innerHeight;
    
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    canvas.style.width = width + 'px';
    canvas.style.height = height + 'px';
    
    ctx.scale(dpr, dpr);

    // Responsive particle count calculation
    let count = Math.floor((width * height) / 18000);
    if (isMobile) count = Math.floor(count / 2.5);
    config.particleCount = Math.min(count, config.maxParticles);
    
    // Smoothly adjust particle count rather than full wipe
    while (particles.length > config.particleCount) {
        particles.pop();
    }
    while (particles.length < config.particleCount) {
        particles.push(new Particle());
    }
}

// Throttled resize
let resizeTimeout;
window.addEventListener('resize', () => {
    clearTimeout(resizeTimeout);
    resizeTimeout = setTimeout(resize, 200);
}, { passive: true });

class Particle {
    constructor() {
        this.x = Math.random() * width;
        this.y = Math.random() * height;
        this.vx = (Math.random() - 0.5) * config.baseSpeed;
        this.vy = (Math.random() - 0.5) * config.baseSpeed;
        this.radius = isMobile ? (Math.random() * 1.5 + 1) : (Math.random() * 2 + 1);
        
        let colorIdx = Math.floor(Math.random() * config.rgbColors.length);
        this.rgb = config.rgbColors[colorIdx];
        
        this.originalVx = this.vx;
        this.originalVy = this.vy;
        this.phase = Math.random() * Math.PI * 2;
    }

    update() {
        this.phase += 0.003;
        // Wave floating
        this.x += this.vx + Math.sin(this.phase) * 0.1;
        this.y += this.vy + Math.cos(this.phase) * 0.1;

        if (mouseActive && !isMobile) {
            let dx = mouseX - this.x;
            let dy = mouseY - this.y;
            let distSq = dx * dx + dy * dy;

            // Use distance squared to avoid expensive Math.sqrt when far away
            if (distSq < 32400) { // 180 squared
                let dist = Math.sqrt(distSq);
                let force = (180 - dist) / 180;
                this.vx -= (dx / dist) * force * 0.05;
                this.vy -= (dy / dist) * force * 0.05;
            } else {
                // Smoothly return to original speed
                this.vx += (this.originalVx - this.vx) * 0.01;
                this.vy += (this.originalVy - this.vy) * 0.01;
            }
        } else {
            this.vx += (this.originalVx - this.vx) * 0.01;
            this.vy += (this.originalVy - this.vy) * 0.01;
        }

        // Speed limit friction (2.0 squared)
        const speedSq = this.vx * this.vx + this.vy * this.vy;
        if(speedSq > 4.0) { 
            this.vx *= 0.92;
            this.vy *= 0.92;
        }

        // Screen wrap
        if (this.x < -30) this.x = width + 30;
        if (this.x > width + 30) this.x = -30;
        if (this.y < -30) this.y = height + 30;
        if (this.y > height + 30) this.y = -30;
    }

    draw(isLogin) {
        ctx.beginPath();
        ctx.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
        // Optimization: Removed heavy ctx.shadowBlur entirely, using raw rgba
        ctx.fillStyle = `rgba(${this.rgb}, ${isLogin ? 0.9 : 0.5})`;
        ctx.fill();
    }
}

// Simple Object Pool for Ripples to avoid garbage collection lag
class RipplePool {
    constructor(size) {
        this.pool = [];
        for(let i=0; i<size; i++) {
            this.pool.push({ active: false, x: 0, y: 0, radius: 0, alpha: 0, rgb: config.rgbColors[0] });
        }
    }
    
    spawn(x, y) {
        for(let i=0; i<this.pool.length; i++) {
            if(!this.pool[i].active) {
                this.pool[i].active = true;
                this.pool[i].x = x;
                this.pool[i].y = y;
                this.pool[i].radius = 0;
                this.pool[i].alpha = 0.8;
                this.pool[i].rgb = config.rgbColors[Math.floor(Math.random() * config.rgbColors.length)];
                return;
            }
        }
    }
}

const ripplePool = new RipplePool(4); // Max 4 concurrent ripples

// Passive listeners for best performance
document.addEventListener('mousemove', (e) => {
    targetMouseX = e.clientX;
    targetMouseY = e.clientY;
    mouseActive = true;
}, { passive: true });

document.addEventListener('mouseleave', () => {
    mouseActive = false;
    targetMouseX = -1000;
    targetMouseY = -1000;
}, { passive: true });

document.addEventListener('click', (e) => {
    if (isMobile) return;
    
    ripplePool.spawn(e.clientX, e.clientY);
    
    // Burst particles outwards efficiently
    particles.forEach(p => {
        let dx = e.clientX - p.x;
        let dy = e.clientY - p.y;
        let distSq = dx * dx + dy * dy;
        if(distSq < 40000) { // 200 squared
            let dist = Math.sqrt(distSq);
            let force = (200 - dist) / 200;
            p.vx -= (dx / dist) * force * 3;
            p.vy -= (dy / dist) * force * 3;
        }
    });
}, { passive: true });

// Cache elements globally or outside loop
const loginSection = document.getElementById('login-section');

function animate() {
    if (!isTabActive) return;

    // Check inline style which is set by app.js, extremely fast
    const isLogin = loginSection && loginSection.style.display !== 'none';
    
    // Use clearRect instead of globalCompositeOperation trick for immense GPU savings
    ctx.clearRect(0, 0, width, height);

    // Smooth cursor interpolation (lerp)
    if (mouseActive) {
        mouseX += (targetMouseX - mouseX) * 0.12;
        mouseY += (targetMouseY - mouseY) * 0.12;
    } else {
        mouseX = -1000;
        mouseY = -1000;
    }

    // Draw lines (optimized double loop with distance bounds checking)
    ctx.lineWidth = isLogin ? 1.0 : 0.6;
    let distThresholdSq = config.connectionDistance * config.connectionDistance;
    
    for (let i = 0; i < particles.length; i++) {
        let p1 = particles[i];
        p1.update();
        p1.draw(isLogin);

        // Connect particles
        for (let j = i + 1; j < particles.length; j++) {
            let p2 = particles[j];
            
            // Fast bounding box check before expensive Math
            if (Math.abs(p1.x - p2.x) > config.connectionDistance || 
                Math.abs(p1.y - p2.y) > config.connectionDistance) {
                continue;
            }

            let dx = p1.x - p2.x;
            let dy = p1.y - p2.y;
            let distSq = dx * dx + dy * dy;

            if (distSq < distThresholdSq) {
                let dist = Math.sqrt(distSq);
                let alpha = 1 - (dist / config.connectionDistance);
                ctx.beginPath();
                ctx.moveTo(p1.x, p1.y);
                ctx.lineTo(p2.x, p2.y);
                ctx.strokeStyle = `rgba(20, 184, 166, ${alpha * (isLogin ? 0.4 : 0.12)})`;
                ctx.stroke();
            }
        }
    }

    // Draw ripples
    for (let i = 0; i < ripplePool.pool.length; i++) {
        let r = ripplePool.pool[i];
        if(r.active) {
            r.radius += 2.5;
            r.alpha -= 0.025;
            
            ctx.beginPath();
            ctx.arc(r.x, r.y, r.radius, 0, Math.PI * 2);
            ctx.strokeStyle = `rgba(${r.rgb}, ${r.alpha})`;
            ctx.lineWidth = 1.2;
            ctx.stroke();
            
            if (r.alpha <= 0) r.active = false;
        }
    }

    animationFrameId = requestAnimationFrame(animate);
}

// Magnet pull on interactive elements using event capturing (highly efficient)
document.body.addEventListener('mouseenter', (e) => {
    if (isMobile) return;
    const target = e.target;
    if (target.tagName === 'BUTTON' || target.classList?.contains('panel-card') || target.classList?.contains('glass-panel')) {
        const rect = target.getBoundingClientRect();
        const cx = rect.left + rect.width / 2;
        const cy = rect.top + rect.height / 2;
        
        particles.forEach(p => {
            let dx = cx - p.x;
            let dy = cy - p.y;
            let distSq = dx * dx + dy * dy;
            if(distSq < 62500) { // 250 squared
                let dist = Math.sqrt(distSq);
                p.vx += (dx / dist) * 0.1;
                p.vy += (dy / dist) * 0.1;
            }
        });
    }
}, true); // Use capture phase to catch hover on all descending elements without binding multiple events

resize();
animate();
