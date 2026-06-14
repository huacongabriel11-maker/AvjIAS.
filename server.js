require('dotenv').config();
const cluster = require('cluster');
const os = require('os');
const express = require('express');
const cors = require('cors');
const mongoose = require('mongoose');
const rateLimit = require('express-rate-limit');
const crypto = require('crypto');
const axios = require('axios');
const http = require('http');
const https = require('https');
const path = require('path');

const numCPUs = os.cpus().length;

if (cluster.isMaster) {
    console.log(`\n👑 AVJ Core: Iniciando Orquestador Maestro (PID: ${process.pid})`);
    console.log(`Activando ${numCPUs} nodos de procesamiento paralelo...\n`);

    for (let i = 0; i < numCPUs; i++) cluster.fork();

    cluster.on('exit', (worker) => {
        console.warn(`[ALERTA] Nodo ${worker.process.pid} caído. Reiniciando para alta disponibilidad...`);
        cluster.fork();
    });
} else {
    const app = express();
    app.use(express.json({ limit: '500kb' }));
    app.use(cors());

    // SERVIR EL FRONTEND (index.html)
    app.get('/', (req, res) => {
        res.sendFile(path.join(__dirname, 'index.html'));
    });

    // CONEXIÓN A BASE DE DATOS
    const connectDB = async () => {
        try {
            if (!process.env.MONGO_URI) throw new Error("MONGO_URI no definida en .env");
            await mongoose.connect(process.env.MONGO_URI, {
                maxPoolSize: Math.max(10, Math.floor(50 / numCPUs)),
                serverSelectionTimeoutMS: 5000,
            });
            console.log(`[Nodo ${process.pid}] Base de datos conectada.`);
        } catch (error) {
            console.error(`[Nodo ${process.pid}] Error DB:`, error.message);
            // Evitamos hacer crash en dev si MongoDB no está corriendo
            console.log("Ejecutando servidor sin persistencia (Modo Fallback)");
        }
    };
    connectDB();

    // MODELOS DE DATOS
    const ChatSchema = new mongoose.Schema({
        user_code: { type: String, required: true, uppercase: true },
        title: { type: String, default: 'Nueva Sesión SaaS' },
        is_development_session: { type: Boolean, default: false },
        messages: [{
            role: { type: String, enum: ['user', 'assistant', 'system'] },
            content: { type: String },
            timestamp: { type: Date, default: Date.now },
            metadata: { provider: String, model_used: String, time_ms: Number }
        }],
        updated_at: { type: Date, default: Date.now }
    });
    ChatSchema.index({ user_code: 1, updated_at: -1 });
    const Chat = mongoose.models.Chat || mongoose.model('Chat', ChatSchema);

    // SISTEMA DE SEGURIDAD
    const ADMIN_CODE = process.env.ADMIN_CODE || 'AVJ-MASTER-ADMIN-99X';
    const DEV_CODE = 'DEVELOPMENT';
    const validCodesList = Array.from({length: 20}, (_, i) => `AVJ${String(i + 1).padStart(3, '0')}`);
    const activeSessions = new Set(); 

    const secureCompare = (a, b) => {
        if (a.length !== b.length) return false;
        return crypto.timingSafeEqual(Buffer.from(a), Buffer.from(b));
    };

    const authenticate = (req, res, next) => {
        const token = req.headers['x-avj-access-code'];
        if (!token) return res.status(401).json({ error: 'Firma de acceso requerida.' });
        
        const upperToken = token.toUpperCase();

        if (upperToken === DEV_CODE) {
            req.user = { code: 'DEVELOPMENT', isDev: true, role: 'dev' };
            return next();
        }

        if (upperToken.length === ADMIN_CODE.length && secureCompare(upperToken, ADMIN_CODE)) {
            req.user = { code: 'ADMIN', isDev: false, role: 'admin' };
            return next();
        }

        if (validCodesList.includes(upperToken)) {
            activeSessions.add(upperToken);
            if (activeSessions.size > 20) {
                activeSessions.delete(upperToken);
                return res.status(429).json({ error: 'Límite de la red alcanzado (20 usuarios activos).' });
            }
            req.user = { code: upperToken, isDev: false, role: 'user' };
            return next();
        }

        return res.status(403).json({ error: 'Credenciales AVJ inválidas.' });
    };

    const apiLimiter = rateLimit({ windowMs: 60 * 1000, max: 15 });

    // MOTOR IA ORQUESTADOR
    function selectModel(prompt) {
        const p = prompt.toLowerCase();
        if (/(html|css|js|node|python|código|bug)/.test(p)) return { provider: 'OPENAI', model: 'gpt-5.3-codex' };
        if (p.length > 500) return { provider: 'ANTHROPIC', model: 'claude-opus-4-8' };
        return { provider: 'GEMINI', model: 'gemini-3.5-flash' };
    }

    async function executeAI(prompt) {
        const { provider, model } = selectModel(prompt);
        const start = Date.now();
        const controller = new AbortController();

        const timeout = new Promise((_, r) => setTimeout(() => { controller.abort(); r(new Error('TIMEOUT')); }, 4500));
        
        try {
            // AQUÍ INTEGRARÍAS EL SDK REAL DE OpenAI/Gemini/Anthropic
            // Usamos un mock para el ejemplo que devuelve en 1 segundo
            const apiCall = new Promise(res => setTimeout(() => res(`Esta es una respuesta generada en tiempo real por ${model} basada en tu petición: "${prompt}"`), 1000));
            const response = await Promise.race([apiCall, timeout]);
            
            return { response, metadata: { model_used: model, provider, time_ms: Date.now() - start } };
        } catch (err) {
            return { 
                response: "Respuesta de respaldo rápido (Fallback activado por timeout).", 
                metadata: { model_used: 'gemini-3.5-flash-fallback', provider: 'GEMINI', time_ms: Date.now() - start }
            };
        }
    }

    // ENDPOINTS API
    app.post('/api/v1/chat', apiLimiter, authenticate, async (req, res) => {
        const { prompt, chatId } = req.body;
        if (!prompt) return res.status(400).json({ error: 'Prompt requerido.' });

        try {
            const aiRes = await executeAI(prompt);
            const responseChatId = chatId || new mongoose.Types.ObjectId().toString();
            
            res.status(200).json({ chatId: responseChatId, ...aiRes });

            // Guardar en DB en segundo plano
            if (mongoose.connection.readyState === 1) {
                setImmediate(async () => {
                    try {
                        let chat = chatId ? await Chat.findById(chatId) : new Chat({ _id: responseChatId, user_code: req.user.code, is_development_session: req.user.isDev, title: prompt.substring(0,20) });
                        if(chat) {
                            chat.messages.push({ role: 'user', content: prompt });
                            chat.messages.push({ role: 'assistant', content: aiRes.response, metadata: aiRes.metadata });
                            await chat.save();
                        }
                    } catch (e) { console.error('Error DB:', e.message); }
                });
            }
        } catch (error) {
            if (!res.headersSent) res.status(500).json({ error: 'Error del Core IA.' });
        }
    });

    app.listen(3000, () => console.log(`[Nodo ${process.pid}] Servidor operativo en http://localhost:3000`));
                    }
