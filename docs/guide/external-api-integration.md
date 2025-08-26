# 外部 API 使用 Easy LLM CLI 模式完整指南

本指南详细介绍如何通过外部 API 或程序化方式使用 Easy LLM CLI 的 CLI 模式功能，实现自动化集成和高级应用场景。

## 目录

- [1. 概述](#1-概述)
- [2. 非交互模式](#2-非交互模式)
- [3. 交互式进程管理](#3-交互式进程管理)
- [4. HTTP API 包装器](#4-http-api-包装器)
- [5. WebSocket 实时通信](#5-websocket-实时通信)
- [6. 最佳实践](#6-最佳实践)
- [7. 故障排除](#7-故障排除)

## 1. 概述

Easy LLM CLI 提供了多种方式让外部系统集成其强大的 CLI 功能：

- **非交互模式**：适合单次查询和批处理
- **交互式进程管理**：支持持续对话和上下文管理
- **HTTP API 包装**：提供 RESTful 接口
- **WebSocket 通信**：实现实时双向通信

### 1.1 优势对比

| 方式 | 优势 | 适用场景 |
|------|------|----------|
| 非交互模式 | 简单、快速、无状态 | 单次查询、批处理 |
| 交互式进程 | 保持上下文、功能完整 | 持续对话、复杂任务 |
| HTTP API | 标准化、易集成 | Web 应用、微服务 |
| WebSocket | 实时、双向通信 | 实时应用、流式响应 |

## 2. 非交互模式

### 2.1 基本用法

Easy LLM CLI 支持通过命令行参数或管道输入进行非交互式调用：

```bash
# 方式1：使用 --prompt 参数
elc --prompt "分析这个项目的结构"

# 方式2：使用管道输入
echo "解释什么是递归" | elc

# 方式3：组合使用
echo "额外的上下文" | elc --prompt "基于上面的内容，请总结要点"

# 方式4：引用文件
elc --prompt "@package.json 分析这个项目的依赖关系"
```

### 2.2 程序化封装

```javascript
import { spawn } from 'child_process';

class EasyLLMCLIWrapper {
  constructor(options = {}) {
    this.options = {
      model: options.model,
      yolo: options.yolo || false,
      sandbox: options.sandbox || false,
      checkpointing: options.checkpointing || false,
      ...options
    };
  }

  /**
   * 执行单次查询
   * @param {string} prompt - 查询内容
   * @param {Object} options - 选项
   * @returns {Promise<string>} AI 回复
   */
  async query(prompt, options = {}) {
    return new Promise((resolve, reject) => {
      const args = ['--prompt', prompt];
      
      // 添加配置参数
      if (this.options.yolo) args.push('--yolo');
      if (this.options.sandbox) args.push('--sandbox');
      if (this.options.checkpointing) args.push('--checkpointing');
      if (this.options.model) args.push('--model', this.options.model);
      
      const child = spawn('npx', ['easy-llm-cli', ...args], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: options.cwd || process.cwd(),
        env: { ...process.env, ...options.env }
      });

      let stdout = '';
      let stderr = '';

      child.stdout.on('data', (data) => {
        stdout += data.toString();
      });

      child.stderr.on('data', (data) => {
        stderr += data.toString();
      });

      child.on('close', (code) => {
        if (code === 0) {
          resolve(stdout.trim());
        } else {
          reject(new Error(`CLI exited with code ${code}: ${stderr}`));
        }
      });

      child.on('error', reject);

      // 设置超时
      if (options.timeout) {
        setTimeout(() => {
          child.kill();
          reject(new Error('Query timeout'));
        }, options.timeout);
      }
    });
  }

  /**
   * 带文件上下文的查询
   * @param {string} prompt - 查询内容
   * @param {string|string[]} filePaths - 文件路径
   * @returns {Promise<string>} AI 回复
   */
  async queryWithFiles(prompt, filePaths = []) {
    const paths = Array.isArray(filePaths) ? filePaths : [filePaths];
    const fileRefs = paths.map(path => `@${path}`).join(' ');
    const fullPrompt = `${fileRefs} ${prompt}`;
    return this.query(fullPrompt);
  }

  /**
   * 批量处理多个查询
   * @param {string[]} prompts - 查询列表
   * @param {Object} options - 选项
   * @returns {Promise<string[]>} 回复列表
   */
  async batchQuery(prompts, options = {}) {
    const results = [];
    const concurrency = options.concurrency || 3;
    
    for (let i = 0; i < prompts.length; i += concurrency) {
      const batch = prompts.slice(i, i + concurrency);
      const batchResults = await Promise.all(
        batch.map(prompt => this.query(prompt, options))
      );
      results.push(...batchResults);
    }
    
    return results;
  }
}

// 使用示例
async function nonInteractiveExample() {
  const cli = new EasyLLMCLIWrapper({
    yolo: true,
    model: 'gpt-4'
  });

  try {
    // 基本查询
    const response1 = await cli.query('你好，请介绍一下自己');
    console.log('AI回复:', response1);

    // 带文件上下文的查询
    const response2 = await cli.queryWithFiles(
      '请分析这些文件的代码质量',
      ['src/main.js', 'package.json']
    );
    console.log('代码分析:', response2);

    // 批量处理
    const prompts = [
      '解释什么是闭包',
      '什么是异步编程',
      '介绍 Promise 的用法'
    ];
    const responses = await cli.batchQuery(prompts);
    responses.forEach((response, index) => {
      console.log(`问题${index + 1}回复:`, response);
    });

  } catch (error) {
    console.error('错误:', error.message);
  }
}
```

## 3. 交互式进程管理

### 3.1 持久化 CLI 进程

```javascript
import { spawn } from 'child_process';
import { EventEmitter } from 'events';

class InteractiveCLIManager extends EventEmitter {
  constructor(options = {}) {
    super();
    this.options = options;
    this.process = null;
    this.isReady = false;
    this.responseBuffer = '';
    this.currentPromise = null;
    this.messageQueue = [];
    this.isProcessing = false;
  }

  /**
   * 启动 CLI 进程
   */
  async start() {
    return new Promise((resolve, reject) => {
      const args = [];
      if (this.options.yolo) args.push('--yolo');
      if (this.options.model) args.push('--model', this.options.model);
      if (this.options.sandbox) args.push('--sandbox');
      if (this.options.checkpointing) args.push('--checkpointing');

      this.process = spawn('npx', ['easy-llm-cli', ...args], {
        stdio: ['pipe', 'pipe', 'pipe'],
        cwd: this.options.cwd || process.cwd(),
        env: { ...process.env, ...this.options.env }
      });

      this.process.stdout.on('data', (data) => {
        this.handleOutput(data.toString());
      });

      this.process.stderr.on('data', (data) => {
        console.error('CLI stderr:', data.toString());
        this.emit('error', new Error(data.toString()));
      });

      this.process.on('close', (code) => {
        this.isReady = false;
        this.emit('close', code);
      });

      this.process.on('error', (error) => {
        this.emit('error', error);
        reject(error);
      });

      // 等待 CLI 准备就绪
      setTimeout(() => {
        this.isReady = true;
        this.emit('ready');
        resolve();
      }, 3000); // 给足够时间让 CLI 初始化
    });
  }

  /**
   * 处理输出数据
   */
  handleOutput(data) {
    this.responseBuffer += data;
    this.emit('output', data);
    
    // 检测响应结束标志
    if (this.isResponseComplete(data)) {
      if (this.currentPromise) {
        const response = this.cleanResponse(this.responseBuffer);
        this.currentPromise.resolve(response);
        this.currentPromise = null;
        this.responseBuffer = '';
        this.isProcessing = false;
        
        // 处理队列中的下一个消息
        this.processQueue();
      }
    }
  }

  /**
   * 判断响应是否完成
   */
  isResponseComplete(data) {
    // 检测 CLI 提示符或特定结束标志
    return data.includes('> ') || 
           data.includes('Type your message') ||
           data.includes('gemini>') ||
           data.match(/\n\s*$/); // 以换行结束
  }

  /**
   * 清理响应内容
   */
  cleanResponse(response) {
    return response
      .replace(/> /g, '')
      .replace(/Type your message.*$/g, '')
      .replace(/gemini>/g, '')
      .trim();
  }

  /**
   * 处理消息队列
   */
  processQueue() {
    if (this.messageQueue.length > 0 && !this.isProcessing) {
      const { message, resolve, reject } = this.messageQueue.shift();
      this.sendMessageInternal(message, resolve, reject);
    }
  }

  /**
   * 发送消息
   */
  async sendMessage(message) {
    if (!this.isReady || !this.process) {
      throw new Error('CLI process not ready');
    }

    return new Promise((resolve, reject) => {
      if (this.isProcessing) {
        // 如果正在处理，加入队列
        this.messageQueue.push({ message, resolve, reject });
      } else {
        this.sendMessageInternal(message, resolve, reject);
      }
    });
  }

  /**
   * 内部发送消息方法
   */
  sendMessageInternal(message, resolve, reject) {
    this.isProcessing = true;
    this.currentPromise = { resolve, reject };
    this.responseBuffer = '';
    
    this.process.stdin.write(message + '\n');
    
    // 设置超时
    setTimeout(() => {
      if (this.currentPromise) {
        this.currentPromise.reject(new Error('Response timeout'));
        this.currentPromise = null;
        this.isProcessing = false;
        this.processQueue();
      }
    }, 60000); // 60秒超时
  }

  /**
   * 发送斜杠命令
   */
  async sendSlashCommand(command) {
    return this.sendMessage(`/${command}`);
  }

  /**
   * 保存对话
   */
  async saveConversation(tag) {
    return this.sendSlashCommand(`chat save ${tag}`);
  }

  /**
   * 恢复对话
   */
  async resumeConversation(tag) {
    return this.sendSlashCommand(`chat resume ${tag}`);
  }

  /**
   * 列出保存的对话
   */
  async listConversations() {
    return this.sendSlashCommand('chat list');
  }

  /**
   * 压缩对话历史
   */
  async compressHistory() {
    return this.sendSlashCommand('compress');
  }

  /**
   * 清屏
   */
  async clearScreen() {
    return this.sendSlashCommand('clear');
  }

  /**
   * 引用文件
   */
  async queryWithFileReference(files, question) {
    const fileList = Array.isArray(files) ? files : [files];
    const fileRefs = fileList.map(f => `@${f}`).join(' ');
    return this.sendMessage(`${fileRefs} ${question}`);
  }

  /**
   * 执行 Shell 命令
   */
  async executeShellCommand(command) {
    return this.sendMessage(`!${command}`);
  }

  /**
   * 获取工具列表
   */
  async getTools() {
    return this.sendSlashCommand('tools');
  }

  /**
   * 获取统计信息
   */
  async getStats() {
    return this.sendSlashCommand('stats');
  }

  /**
   * 关闭进程
   */
  async close() {
    if (this.process) {
      this.process.stdin.end();
      this.process.kill();
      this.process = null;
      this.isReady = false;
    }
  }
}

// 使用示例
async function interactiveExample() {
  const cliManager = new InteractiveCLIManager({
    yolo: true,
    model: 'gpt-4'
  });

  // 监听事件
  cliManager.on('ready', () => {
    console.log('CLI 已准备就绪');
  });

  cliManager.on('output', (data) => {
    console.log('输出:', data);
  });

  cliManager.on('error', (error) => {
    console.error('错误:', error);
  });

  try {
    await cliManager.start();

    // 发送消息
    const response1 = await cliManager.sendMessage('你好，我是开发者张三');
    console.log('回复1:', response1);

    // 继续对话（有上下文）
    const response2 = await cliManager.sendMessage('我刚才说我叫什么？');
    console.log('回复2:', response2);

    // 保存对话
    await cliManager.saveConversation('dev-session');

    // 引用文件
    const response3 = await cliManager.queryWithFileReference(
      'package.json',
      '这个项目使用了什么技术栈？'
    );
    console.log('文件分析:', response3);

    // 执行 Shell 命令
    const shellResult = await cliManager.executeShellCommand('ls -la');
    console.log('Shell 结果:', shellResult);

  } catch (error) {
    console.error('错误:', error);
  } finally {
    await cliManager.close();
  }
}
```

## 4. HTTP API 包装器

### 4.1 Express 服务器

```javascript
import express from 'express';
import cors from 'cors';
import { InteractiveCLIManager } from './cli-manager.js';

class CLIHTTPService {
  constructor(port = 3000) {
    this.app = express();
    this.port = port;
    this.sessions = new Map(); // 存储会话
    this.setupMiddleware();
    this.setupRoutes();
  }

  setupMiddleware() {
    this.app.use(cors());
    this.app.use(express.json({ limit: '10mb' }));
    this.app.use(express.urlencoded({ extended: true }));
    
    // 请求日志
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} ${req.method} ${req.path}`);
      next();
    });
  }

  setupRoutes() {
    // 健康检查
    this.app.get('/health', (req, res) => {
      res.json({ status: 'ok', timestamp: new Date().toISOString() });
    });

    // 创建新会话
    this.app.post('/sessions', async (req, res) => {
      try {
        const { sessionId, options = {} } = req.body;
        
        if (!sessionId) {
          return res.status(400).json({ error: 'sessionId is required' });
        }

        if (this.sessions.has(sessionId)) {
          return res.status(400).json({ error: 'Session already exists' });
        }

        const cliManager = new InteractiveCLIManager(options);
        await cliManager.start();
        
        this.sessions.set(sessionId, {
          cliManager,
          createdAt: new Date(),
          lastUsed: new Date()
        });

        res.json({ 
          sessionId, 
          status: 'created',
          createdAt: new Date().toISOString()
        });
      } catch (error) {
        console.error('Create session error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    // 获取会话信息
    this.app.get('/sessions/:sessionId', (req, res) => {
      const { sessionId } = req.params;
      const session = this.sessions.get(sessionId);
      
      if (!session) {
        return res.status(404).json({ error: 'Session not found' });
      }

      res.json({
        sessionId,
        createdAt: session.createdAt.toISOString(),
        lastUsed: session.lastUsed.toISOString(),
        status: 'active'
      });
    });

    // 发送消息
    this.app.post('/sessions/:sessionId/messages', async (req, res) => {
      try {
        const { sessionId } = req.params;
        const { message, files, timeout = 60000 } = req.body;

        if (!message) {
          return res.status(400).json({ error: 'message is required' });
        }

        const session = this.sessions.get(sessionId);
        if (!session) {
          return res.status(404).json({ error: 'Session not found' });
        }

        session.lastUsed = new Date();

        let response;
        if (files && files.length > 0) {
          response = await session.cliManager.queryWithFileReference(files, message);
        } else {
          response = await session.cliManager.sendMessage(message);
        }

        res.json({ 
          response,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        console.error('Send message error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    // 执行斜杠命令
    this.app.post('/sessions/:sessionId/commands', async (req, res) => {
      try {
        const { sessionId } = req.params;
        const { command } = req.body;

        if (!command) {
          return res.status(400).json({ error: 'command is required' });
        }

        const session = this.sessions.get(sessionId);
        if (!session) {
          return res.status(404).json({ error: 'Session not found' });
        }

        session.lastUsed = new Date();

        const response = await session.cliManager.sendSlashCommand(command);
        res.json({ 
          response,
          command,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        console.error('Execute command error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    // 对话管理
    this.app.post('/sessions/:sessionId/conversations/:action', async (req, res) => {
      try {
        const { sessionId, action } = req.params;
        const { tag } = req.body;

        const session = this.sessions.get(sessionId);
        if (!session) {
          return res.status(404).json({ error: 'Session not found' });
        }

        session.lastUsed = new Date();

        let response;
        switch (action) {
          case 'save':
            if (!tag) {
              return res.status(400).json({ error: 'tag is required for save action' });
            }
            response = await session.cliManager.saveConversation(tag);
            break;
          case 'resume':
            if (!tag) {
              return res.status(400).json({ error: 'tag is required for resume action' });
            }
            response = await session.cliManager.resumeConversation(tag);
            break;
          case 'list':
            response = await session.cliManager.listConversations();
            break;
          case 'compress':
            response = await session.cliManager.compressHistory();
            break;
          default:
            return res.status(400).json({ error: 'Invalid action' });
        }

        res.json({ 
          response,
          action,
          tag,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        console.error('Conversation management error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    // 删除会话
    this.app.delete('/sessions/:sessionId', async (req, res) => {
      try {
        const { sessionId } = req.params;
        const session = this.sessions.get(sessionId);
        
        if (session) {
          await session.cliManager.close();
          this.sessions.delete(sessionId);
        }

        res.json({ 
          status: 'deleted',
          sessionId,
          timestamp: new Date().toISOString()
        });
      } catch (error) {
        console.error('Delete session error:', error);
        res.status(500).json({ error: error.message });
      }
    });

    // 获取会话列表
    this.app.get('/sessions', (req, res) => {
      const sessions = Array.from(this.sessions.entries()).map(([id, session]) => ({
        sessionId: id,
        createdAt: session.createdAt.toISOString(),
        lastUsed: session.lastUsed.toISOString()
      }));
      
      res.json({ 
        sessions,
        count: sessions.length,
        timestamp: new Date().toISOString()
      });
    });

    // 错误处理中间件
    this.app.use((error, req, res, next) => {
      console.error('Unhandled error:', error);
      res.status(500).json({ 
        error: 'Internal server error',
        timestamp: new Date().toISOString()
      });
    });
  }

  start() {
    this.app.listen(this.port, () => {
      console.log(`CLI HTTP Service running on port ${this.port}`);
    });

    // 定期清理过期会话
    setInterval(() => {
      this.cleanupExpiredSessions();
    }, 5 * 60 * 1000); // 每5分钟清理一次
  }

  cleanupExpiredSessions() {
    const now = new Date();
    const expireTime = 30 * 60 * 1000; // 30分钟过期

    for (const [sessionId, session] of this.sessions.entries()) {
      if (now - session.lastUsed > expireTime) {
        console.log(`Cleaning up expired session: ${sessionId}`);
        session.cliManager.close();
        this.sessions.delete(sessionId);
      }
    }
  }
}

// 启动服务
const service = new CLIHTTPService(3000);
service.start();
```

### 4.2 HTTP 客户端

```javascript
class CLIHTTPClient {
  constructor(baseURL = 'http://localhost:3000') {
    this.baseURL = baseURL;
  }

  async request(method, path, data = null) {
    const url = `${this.baseURL}${path}`;
    const options = {
      method,
      headers: { 'Content-Type': 'application/json' }
    };

    if (data) {
      options.body = JSON.stringify(data);
    }

    const response = await fetch(url, options);
    const result = await response.json();

    if (!response.ok) {
      throw new Error(result.error || `HTTP ${response.status}`);
    }

    return result;
  }

  async createSession(sessionId, options = {}) {
    return this.request('POST', '/sessions', { sessionId, options });
  }

  async getSession(sessionId) {
    return this.request('GET', `/sessions/${sessionId}`);
  }

  async sendMessage(sessionId, message, files = []) {
    return this.request('POST', `/sessions/${sessionId}/messages`, {
      message,
      files
    });
  }

  async executeCommand(sessionId, command) {
    return this.request('POST', `/sessions/${sessionId}/commands`, {
      command
    });
  }

  async saveConversation(sessionId, tag) {
    return this.request('POST', `/sessions/${sessionId}/conversations/save`, {
      tag
    });
  }

  async resumeConversation(sessionId, tag) {
    return this.request('POST', `/sessions/${sessionId}/conversations/resume`, {
      tag
    });
  }

  async listConversations(sessionId) {
    return this.request('POST', `/sessions/${sessionId}/conversations/list`);
  }

  async compressHistory(sessionId) {
    return this.request('POST', `/sessions/${sessionId}/conversations/compress`);
  }

  async deleteSession(sessionId) {
    return this.request('DELETE', `/sessions/${sessionId}`);
  }

  async listSessions() {
    return this.request('GET', '/sessions');
  }

  async healthCheck() {
    return this.request('GET', '/health');
  }
}

// 使用示例
async function httpClientExample() {
  const client = new CLIHTTPClient();

  try {
    // 健康检查
    await client.healthCheck();
    console.log('服务正常');

    // 创建会话
    await client.createSession('my-session', { 
      yolo: true,
      model: 'gpt-4'
    });
    console.log('会话已创建');

    // 发送消息
    const response1 = await client.sendMessage('my-session', '你好，我是开发者');
    console.log('回复:', response1.response);

    // 继续对话
    const response2 = await client.sendMessage('my-session', '我刚才说我是什么职业？');
    console.log('回复:', response2.response);

    // 保存对话
    await client.saveConversation('my-session', 'my-conversation');
    console.log('对话已保存');

    // 文件分析
    const response3 = await client.sendMessage(
      'my-session', 
      '分析这个文件的内容', 
      ['package.json']
    );
    console.log('文件分析:', response3.response);

    // 执行命令
    const toolsResult = await client.executeCommand('my-session', 'tools');
    console.log('可用工具:', toolsResult.response);

  } catch (error) {
    console.error('错误:', error.message);
  } finally {
    // 清理会话
    try {
      await client.deleteSession('my-session');
      console.log('会话已删除');
    } catch (error) {
      console.error('删除会话失败:', error.message);
    }
  }
}
```

## 5. WebSocket 实时通信

### 5.1 WebSocket 服务器

```javascript
import WebSocket from 'ws';
import { InteractiveCLIManager } from './cli-manager.js';

class CLIWebSocketService {
  constructor(port = 8080) {
    this.wss = new WebSocket.Server({ port });
    this.sessions = new Map();
    this.connections = new Map(); // WebSocket 连接映射
    this.setupWebSocket();
    console.log(`WebSocket 服务运行在端口 ${port}`);
  }

  setupWebSocket() {
    this.wss.on('connection', (ws, req) => {
      const connectionId = this.generateConnectionId();
      this.connections.set(connectionId, ws);
      
      console.log(`新的 WebSocket 连接: ${connectionId}`);

      // 发送连接确认
      ws.send(JSON.stringify({
        type: 'connection_established',
        connectionId,
        timestamp: new Date().toISOString()
      }));

      ws.on('message', async (data) => {
        try {
          const message = JSON.parse(data.toString());
          await this.handleMessage(ws, connectionId, message);
        } catch (error) {
          console.error('消息处理错误:', error);
          ws.send(JSON.stringify({
            type: 'error',
            error: error.message,
            timestamp: new Date().toISOString()
          }));
        }
      });

      ws.on('close', () => {
        console.log(`WebSocket 连接关闭: ${connectionId}`);
        this.cleanup(connectionId);
      });

      ws.on('error', (error) => {
        console.error(`WebSocket 错误 ${connectionId}:`, error);
        this.cleanup(connectionId);
      });
    });
  }

  generateConnectionId() {
    return `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  async handleMessage(ws, connectionId, message) {
    const { type, sessionId, data, requestId } = message;

    try {
      let response;
      
      switch (type) {
        case 'create_session':
          response = await this.createSession(connectionId, sessionId, data?.options || {});
          break;
        
        case 'send_message':
          response = await this.sendMessage(sessionId, data.message, data.files);
          break;
        
        case 'execute_command':
          response = await this.executeCommand(sessionId, data.command);
          break;
        
        case 'save_conversation':
          response = await this.saveConversation(sessionId, data.tag);
          break;
        
        case 'resume_conversation':
          response = await this.resumeConversation(sessionId, data.tag);
          break;
        
        case 'list_conversations':
          response = await this.listConversations(sessionId);
          break;
        
        case 'get_session_info':
          response = await this.getSessionInfo(sessionId);
          break;
        
        case 'delete_session':
          response = await this.deleteSession(connectionId, sessionId);
          break;
        
        default:
          throw new Error(`未知消息类型: ${type}`);
      }

      // 发送响应
      ws.send(JSON.stringify({
        type: `${type}_response`,
        sessionId,
        requestId,
        data: response,
        timestamp: new Date().toISOString()
      }));

    } catch (error) {
      // 发送错误响应
      ws.send(JSON.stringify({
        type: 'error',
        sessionId,
        requestId,
        error: error.message,
        timestamp: new Date().toISOString()
      }));
    }
  }

  async createSession(connectionId, sessionId, options = {}) {
    if (this.sessions.has(sessionId)) {
      throw new Error('会话已存在');
    }

    const cliManager = new InteractiveCLIManager(options);
    
    // 监听 CLI 输出事件
    cliManager.on('output', (data) => {
      const ws = this.connections.get(connectionId);
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({
          type: 'cli_output',
          sessionId,
          data,
          timestamp: new Date().toISOString()
        }));
      }
    });

    await cliManager.start();
    
    this.sessions.set(sessionId, {
      cliManager,
      connectionId,
      createdAt: new Date(),
      lastUsed: new Date()
    });

    return {
      sessionId,
      status: 'created',
      createdAt: new Date().toISOString()
    };
  }

  async sendMessage(sessionId, message, files = []) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    session.lastUsed = new Date();

    let response;
    if (files && files.length > 0) {
      response = await session.cliManager.queryWithFileReference(files, message);
    } else {
      response = await session.cliManager.sendMessage(message);
    }

    return { response };
  }

  async executeCommand(sessionId, command) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    session.lastUsed = new Date();
    const response = await session.cliManager.sendSlashCommand(command);
    
    return { response, command };
  }

  async saveConversation(sessionId, tag) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    session.lastUsed = new Date();
    const response = await session.cliManager.saveConversation(tag);
    
    return { response, tag };
  }

  async resumeConversation(sessionId, tag) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    session.lastUsed = new Date();
    const response = await session.cliManager.resumeConversation(tag);
    
    return { response, tag };
  }

  async listConversations(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    session.lastUsed = new Date();
    const response = await session.cliManager.listConversations();
    
    return { response };
  }

  async getSessionInfo(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session) {
      throw new Error('会话不存在');
    }

    return {
      sessionId,
      createdAt: session.createdAt.toISOString(),
      lastUsed: session.lastUsed.toISOString(),
      status: 'active'
    };
  }

  async deleteSession(connectionId, sessionId) {
    const session = this.sessions.get(sessionId);
    if (session) {
      await session.cliManager.close();
      this.sessions.delete(sessionId);
    }

    return { status: 'deleted', sessionId };
  }

  cleanup(connectionId) {
    this.connections.delete(connectionId);
    
    // 清理关联的会话
    for (const [sessionId, session] of this.sessions.entries()) {
      if (session.connectionId === connectionId) {
        session.cliManager.close();
        this.sessions.delete(sessionId);
        console.log(`清理会话: ${sessionId}`);
      }
    }
  }
}

// 启动 WebSocket 服务
const wsService = new CLIWebSocketService(8080);
```

### 5.2 WebSocket 客户端

```javascript
import WebSocket from 'ws';
import { EventEmitter } from 'events';

class CLIWebSocketClient extends EventEmitter {
  constructor(url = 'ws://localhost:8080') {
    super();
    this.url = url;
    this.ws = null;
    this.connectionId = null;
    this.requestId = 0;
    this.pendingRequests = new Map();
    this.isConnected = false;
  }

  async connect() {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.url);

      this.ws.on('open', () => {
        console.log('WebSocket 连接已建立');
        this.isConnected = true;
        this.emit('connected');
      });

      this.ws.on('message', (data) => {
        try {
          const message = JSON.parse(data.toString());
          this.handleMessage(message);
        } catch (error) {
          console.error('消息解析错误:', error);
        }
      });

      this.ws.on('close', () => {
        console.log('WebSocket 连接已关闭');
        this.isConnected = false;
        this.emit('disconnected');
      });

      this.ws.on('error', (error) => {
        console.error('WebSocket 错误:', error);
        this.emit('error', error);
        reject(error);
      });

      // 等待连接建立确认
      this.once('connection_established', () => {
        resolve();
      });
    });
  }

  handleMessage(message) {
    const { type, requestId, sessionId, data, error } = message;

    switch (type) {
      case 'connection_established':
        this.connectionId = message.connectionId;
        this.emit('connection_established');
        break;

      case 'cli_output':
        this.emit('cli_output', { sessionId, data });
        break;

      case 'error':
        if (requestId && this.pendingRequests.has(requestId)) {
          const { reject } = this.pendingRequests.get(requestId);
          this.pendingRequests.delete(requestId);
          reject(new Error(error));
        } else {
          this.emit('error', new Error(error));
        }
        break;

      default:
        // 处理响应消息
        if (requestId && this.pendingRequests.has(requestId)) {
          const { resolve } = this.pendingRequests.get(requestId);
          this.pendingRequests.delete(requestId);
          resolve(data);
        }
        break;
    }
  }

  async sendRequest(type, sessionId, data = {}) {
    if (!this.isConnected) {
      throw new Error('WebSocket 未连接');
    }

    return new Promise((resolve, reject) => {
      const requestId = ++this.requestId;
      
      this.pendingRequests.set(requestId, { resolve, reject });

      const message = {
        type,
        sessionId,
        requestId,
        data
      };

      this.ws.send(JSON.stringify(message));

      // 设置超时
      setTimeout(() => {
        if (this.pendingRequests.has(requestId)) {
          this.pendingRequests.delete(requestId);
          reject(new Error('请求超时'));
        }
      }, 60000);
    });
  }

  async createSession(sessionId, options = {}) {
    return this.sendRequest('create_session', sessionId, { options });
  }

  async sendMessage(sessionId, message, files = []) {
    return this.sendRequest('send_message', sessionId, { message, files });
  }

  async executeCommand(sessionId, command) {
    return this.sendRequest('execute_command', sessionId, { command });
  }

  async saveConversation(sessionId, tag) {
    return this.sendRequest('save_conversation', sessionId, { tag });
  }

  async resumeConversation(sessionId, tag) {
    return this.sendRequest('resume_conversation', sessionId, { tag });
  }

  async listConversations(sessionId) {
    return this.sendRequest('list_conversations', sessionId);
  }

  async getSessionInfo(sessionId) {
    return this.sendRequest('get_session_info', sessionId);
  }

  async deleteSession(sessionId) {
    return this.sendRequest('delete_session', sessionId);
  }

  disconnect() {
    if (this.ws) {
      this.ws.close();
    }
  }
}

// 使用示例
async function webSocketExample() {
  const client = new CLIWebSocketClient();

  // 监听事件
  client.on('connected', () => {
    console.log('已连接到服务器');
  });

  client.on('cli_output', ({ sessionId, data }) => {
    console.log(`会话 ${sessionId} 输出:`, data);
  });

  client.on('error', (error) => {
    console.error('客户端错误:', error);
  });

  try {
    await client.connect();

    // 创建会话
    const createResult = await client.createSession('my-session', {
      yolo: true,
      model: 'gpt-4'
    });
    console.log('会话创建结果:', createResult);

    // 发送消息
    const response1 = await client.sendMessage('my-session', '你好，我是开发者');
    console.log('回复1:', response1.response);

    // 继续对话
    const response2 = await client.sendMessage('my-session', '我刚才说我是什么职业？');
    console.log('回复2:', response2.response);

    // 保存对话
    const saveResult = await client.saveConversation('my-session', 'my-conversation');
    console.log('保存结果:', saveResult);

  } catch (error) {
    console.error('错误:', error);
  } finally {
    client.disconnect();
  }
}
```

## 6. 最佳实践

### 6.1 环境变量配置

```javascript
// config.js
export const CLIConfig = {
  // 从环境变量读取配置
  customLLM: {
    enabled: process.env.USE_CUSTOM_LLM === 'true',
    apiKey: process.env.CUSTOM_LLM_API_KEY,
    endpoint: process.env.CUSTOM_LLM_ENDPOINT,
    model: process.env.CUSTOM_LLM_MODEL_NAME,
    temperature: parseFloat(process.env.CUSTOM_LLM_TEMPERATURE || '0'),
    maxTokens: parseInt(process.env.CUSTOM_LLM_MAX_TOKENS || '8192'),
    topP: parseFloat(process.env.CUSTOM_LLM_TOP_P || '1')
  },
  
  // CLI 选项
  options: {
    yolo: process.env.CLI_YOLO === 'true',
    sandbox: process.env.CLI_SANDBOX === 'true',
    checkpointing: process.env.CLI_CHECKPOINTING === 'true',
    debug: process.env.CLI_DEBUG === 'true'
  },
  
  // 超时设置
  timeouts: {
    startup: parseInt(process.env.CLI_STARTUP_TIMEOUT || '30000'),
    response: parseInt(process.env.CLI_RESPONSE_TIMEOUT || '60000'),
    cleanup: parseInt(process.env.CLI_CLEANUP_TIMEOUT || '5000')
  }
};

// 设置环境变量的辅助函数
export function setupEnvironment(config) {
  if (config.customLLM.enabled) {
    process.env.USE_CUSTOM_LLM = 'true';
    process.env.CUSTOM_LLM_API_KEY = config.customLLM.apiKey;
    process.env.CUSTOM_LLM_ENDPOINT = config.customLLM.endpoint;
    process.env.CUSTOM_LLM_MODEL_NAME = config.customLLM.model;
    process.env.CUSTOM_LLM_TEMPERATURE = config.customLLM.temperature.toString();
    process.env.CUSTOM_LLM_MAX_TOKENS = config.customLLM.maxTokens.toString();
    process.env.CUSTOM_LLM_TOP_P = config.customLLM.topP.toString();
  }
}
```

### 6.2 错误处理和重试机制

```javascript
class CLIErrorHandler {
  static isRetryableError(error) {
    const retryablePatterns = [
      /timeout/i,
      /connection/i,
      /network/i,
      /ECONNRESET/i,
      /ENOTFOUND/i
    ];
    
    return retryablePatterns.some(pattern => pattern.test(error.message));
  }

  static async withRetry(operation, maxRetries = 3, delay = 1000) {
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return await operation();
      } catch (error) {
        console.error(`尝试 ${attempt}/${maxRetries} 失败:`, error.message);
        
        if (attempt === maxRetries || !this.isRetryableError(error)) {
          throw error;
        }
        
        // 指数退避
        const waitTime = delay * Math.pow(2, attempt - 1);
        console.log(`等待 ${waitTime}ms 后重试...`);
        await new Promise(resolve => setTimeout(resolve, waitTime));
      }
    }
  }

  static handleCLIError(error, context = '') {
    console.error(`CLI Error ${context}:`, error);
    
    if (error.message.includes('ENOENT')) {
      throw new Error('Easy LLM CLI 未找到。请先安装: npm install -g easy-llm-cli');
    }
    
    if (error.message.includes('authentication')) {
      throw new Error('认证失败。请检查您的 API 密钥配置。');
    }
    
    if (error.message.includes('rate limit')) {
      throw new Error('API 速率限制。请稍后重试。');
    }
    
    if (error.message.includes('quota')) {
      throw new Error('API 配额已用完。请检查您的账户余额。');
    }
    
    throw error;
  }
}

// 使用示例
async function robustCLIOperation() {
  try {
    const result = await CLIErrorHandler.withRetry(async () => {
      const cli = new EasyLLMCLIWrapper();
      return await cli.query('测试查询');
    }, 3, 1000);
    
    console.log('操作成功:', result);
  } catch (error) {
    CLIErrorHandler.handleCLIError(error, 'robustCLIOperation');
  }
}
```

### 6.3 性能优化 - 进程池管理

```javascript
class CLIProcessPool {
  constructor(options = {}) {
    this.maxSize = options.maxSize || 5;
    this.minSize = options.minSize || 1;
    this.idleTimeout = options.idleTimeout || 5 * 60 * 1000; // 5分钟
    this.pool = [];
    this.busy = new Set();
    this.waitQueue = [];
    
    // 预热进程池
    this.warmUp();
    
    // 定期清理空闲进程
    setInterval(() => this.cleanup(), 60000);
  }

  async warmUp() {
    console.log(`预热进程池，创建 ${this.minSize} 个进程...`);
    
    for (let i = 0; i < this.minSize; i++) {
      try {
        const process = await this.createProcess();
        this.pool.push({
          process,
          createdAt: new Date(),
          lastUsed: new Date()
        });
      } catch (error) {
        console.error('进程池预热失败:', error);
      }
    }
    
    console.log(`进程池预热完成，当前进程数: ${this.pool.length}`);
  }

  async createProcess(options = {}) {
    const process = new InteractiveCLIManager(options);
    await process.start();
    return process;
  }

  async getProcess(options = {}) {
    // 查找空闲进程
    const available = this.pool.find(item => !this.busy.has(item.process));
    if (available) {
      this.busy.add(available.process);
      available.lastUsed = new Date();
      return available.process;
    }

    // 如果池未满，创建新进程
    if (this.pool.length < this.maxSize) {
      try {
        const process = await this.createProcess(options);
        const item = {
          process,
          createdAt: new Date(),
          lastUsed: new Date()
        };
        this.pool.push(item);
        this.busy.add(process);
        return process;
      } catch (error) {
        console.error('创建新进程失败:', error);
      }
    }

    // 等待进程可用
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        const index = this.waitQueue.findIndex(item => item.resolve === resolve);
        if (index !== -1) {
          this.waitQueue.splice(index, 1);
        }
        reject(new Error('获取进程超时'));
      }, 30000);

      this.waitQueue.push({ resolve, reject, timeout, options });
    });
  }

  releaseProcess(process) {
    this.busy.delete(process);
    
    // 更新最后使用时间
    const item = this.pool.find(item => item.process === process);
    if (item) {
      item.lastUsed = new Date();
    }
    
    // 处理等待队列
    if (this.waitQueue.length > 0) {
      const { resolve, timeout } = this.waitQueue.shift();
      clearTimeout(timeout);
      this.busy.add(process);
      resolve(process);
    }
  }

  cleanup() {
    const now = new Date();
    const itemsToRemove = [];

    for (const item of this.pool) {
      // 保留最小数量的进程
      if (this.pool.length <= this.minSize) break;
      
      // 移除空闲超时的进程
      if (!this.busy.has(item.process) && 
          (now - item.lastUsed) > this.idleTimeout) {
        itemsToRemove.push(item);
      }
    }

    for (const item of itemsToRemove) {
      console.log('清理空闲进程');
      item.process.close();
      const index = this.pool.indexOf(item);
      if (index !== -1) {
        this.pool.splice(index, 1);
      }
    }
  }

  async destroy() {
    console.log('销毁进程池...');
    
    // 清理等待队列
    for (const { reject, timeout } of this.waitQueue) {
      clearTimeout(timeout);
      reject(new Error('进程池已销毁'));
    }
    this.waitQueue = [];

    // 关闭所有进程
    for (const item of this.pool) {
      try {
        await item.process.close();
      } catch (error) {
        console.error('关闭进程失败:', error);
      }
    }
    
    this.pool = [];
    this.busy.clear();
    
    console.log('进程池已销毁');
  }

  getStats() {
    return {
      total: this.pool.length,
      busy: this.busy.size,
      idle: this.pool.length - this.busy.size,
      waiting: this.waitQueue.length,
      maxSize: this.maxSize,
      minSize: this.minSize
    };
  }
}

// 使用示例
const processPool = new CLIProcessPool({
  maxSize: 5,
  minSize: 2,
  idleTimeout: 5 * 60 * 1000
});

async function useProcessPool() {
  let process;
  try {
    process = await processPool.getProcess({ yolo: true });
    const response = await process.sendMessage('你好');
    console.log('回复:', response);
  } catch (error) {
    console.error('错误:', error);
  } finally {
    if (process) {
      processPool.releaseProcess(process);
    }
  }
}

// 优雅关闭
process.on('SIGINT', async () => {
  console.log('正在关闭进程池...');
  await processPool.destroy();
  process.exit(0);
});
```

### 6.4 监控和日志

```javascript
import fs from 'fs';
import path from 'path';

class CLIMonitor {
  constructor(options = {}) {
    this.logDir = options.logDir || './logs';
    this.enableMetrics = options.enableMetrics !== false;
    this.metrics = {
      requests: 0,
      errors: 0,
      totalResponseTime: 0,
      sessions: 0
    };
    
    this.ensureLogDir();
  }

  ensureLogDir() {
    if (!fs.existsSync(this.logDir)) {
      fs.mkdirSync(this.logDir, { recursive: true });
    }
  }

  log(level, message, data = {}) {
    const timestamp = new Date().toISOString();
    const logEntry = {
      timestamp,
      level,
      message,
      ...data
    };

    // 控制台输出
    console.log(`[${timestamp}] ${level.toUpperCase()}: ${message}`);

    // 文件输出
    const logFile = path.join(this.logDir, `cli-${new Date().toISOString().split('T')[0]}.log`);
    fs.appendFileSync(logFile, JSON.stringify(logEntry) + '\n');
  }

  info(message, data) {
    this.log('info', message, data);
  }

  error(message, error, data = {}) {
    this.log('error', message, { 
      error: error.message, 
      stack: error.stack, 
      ...data 
    });
    
    if (this.enableMetrics) {
      this.metrics.errors++;
    }
  }

  warn(message, data) {
    this.log('warn', message, data);
  }

  debug(message, data) {
    this.log('debug', message, data);
  }

  recordRequest(duration) {
    if (this.enableMetrics) {
      this.metrics.requests++;
      this.metrics.totalResponseTime += duration;
    }
  }

  recordSession() {
    if (this.enableMetrics) {
      this.metrics.sessions++;
    }
  }

  getMetrics() {
    return {
      ...this.metrics,
      averageResponseTime: this.metrics.requests > 0 
        ? this.metrics.totalResponseTime / this.metrics.requests 
        : 0,
      errorRate: this.metrics.requests > 0 
        ? (this.metrics.errors / this.metrics.requests) * 100 
        : 0
    };
  }

  resetMetrics() {
    this.metrics = {
      requests: 0,
      errors: 0,
      totalResponseTime: 0,
      sessions: 0
    };
  }
}

// 全局监控实例
export const monitor = new CLIMonitor();

// 性能监控装饰器
export function withMonitoring(target, propertyKey, descriptor) {
  const originalMethod = descriptor.value;

  descriptor.value = async function(...args) {
    const startTime = Date.now();
    
    try {
      monitor.info(`开始执行 ${propertyKey}`, { args: args.length });
      const result = await originalMethod.apply(this, args);
      
      const duration = Date.now() - startTime;
      monitor.recordRequest(duration);
      monitor.info(`完成执行 ${propertyKey}`, { duration });
      
      return result;
    } catch (error) {
      const duration = Date.now() - startTime;
      monitor.error(`执行 ${propertyKey} 失败`, error, { duration });
      throw error;
    }
  };

  return descriptor;
}
```

## 7. 故障排除

### 7.1 常见问题

#### 问题 1: CLI 进程无法启动

**症状**: 进程创建失败，提示 "ENOENT" 错误

**解决方案**:
```javascript
// 检查 CLI 是否已安装
async function checkCLIInstallation() {
  try {
    const { spawn } = require('child_process');
    const child = spawn('npx', ['easy-llm-cli', '--help'], { stdio: 'pipe' });
    
    return new Promise((resolve) => {
      child.on('close', (code) => {
        resolve(code === 0);
      });
      child.on('error', () => {
        resolve(false);
      });
    });
  } catch (error) {
    return false;
  }
}

// 使用前检查
if (!(await checkCLIInstallation())) {
  throw new Error('请先安装 Easy LLM CLI: npm install -g easy-llm-cli');
}
```

#### 问题 2: 响应超时

**症状**: 长时间无响应，最终超时

**解决方案**:
```javascript
// 增加超时时间并添加进度监控
class TimeoutManager {
  constructor(timeout = 60000) {
    this.timeout = timeout;
  }

  async withTimeout(promise, onProgress) {
    let progressTimer;
    
    const timeoutPromise = new Promise((_, reject) => {
      const timer = setTimeout(() => {
        if (progressTimer) clearInterval(progressTimer);
        reject(new Error(`操作超时 (${this.timeout}ms)`));
      }, this.timeout);
      
      // 进度监控
      if (onProgress) {
        let elapsed = 0;
        progressTimer = setInterval(() => {
          elapsed += 1000;
          onProgress(elapsed, this.timeout);
        }, 1000);
      }
    });

    try {
      const result = await Promise.race([promise, timeoutPromise]);
      if (progressTimer) clearInterval(progressTimer);
      return result;
    } catch (error) {
      if (progressTimer) clearInterval(progressTimer);
      throw error;
    }
  }
}
```

#### 问题 3: 内存泄漏

**症状**: 长时间运行后内存使用持续增长

**解决方案**:
```javascript
// 内存监控和清理
class MemoryManager {
  constructor(options = {}) {
    this.maxMemoryMB = options.maxMemoryMB || 512;
    this.checkInterval = options.checkInterval || 30000;
    this.startMonitoring();
  }

  startMonitoring() {
    setInterval(() => {
      const usage = process.memoryUsage();
      const heapUsedMB = usage.heapUsed / 1024 / 1024;
      
      console.log(`内存使用: ${heapUsedMB.toFixed(2)} MB`);
      
      if (heapUsedMB > this.maxMemoryMB) {
        console.warn('内存使用过高，触发垃圾回收');
        if (global.gc) {
          global.gc();
        }
      }
    }, this.checkInterval);
  }

  getMemoryUsage() {
    const usage = process.memoryUsage();
    return {
      heapUsed: Math.round(usage.heapUsed / 1024 / 1024),
      heapTotal: Math.round(usage.heapTotal / 1024 / 1024),
      external: Math.round(usage.external / 1024 / 1024),
      rss: Math.round(usage.rss / 1024 / 1024)
    };
  }
}
```

### 7.2 调试技巧

```javascript
// 调试模式配置
const DEBUG_CONFIG = {
  enabled: process.env.DEBUG === '1',
  logLevel: process.env.LOG_LEVEL || 'info',
  saveResponses: process.env.SAVE_RESPONSES === '1'
};

class DebugHelper {
  static log(level, message, data) {
    if (!DEBUG_CONFIG.enabled) return;
    
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] [${level.toUpperCase()}] ${message}`);
    
    if (data) {
      console.log(JSON.stringify(data, null, 2));
    }
  }

  static saveResponse(sessionId, request, response) {
    if (!DEBUG_CONFIG.saveResponses) return;
    
    const filename = `debug_${sessionId}_${Date.now()}.json`;
    const debugData = {
      timestamp: new Date().toISOString(),
      sessionId,
      request,
      response
    };
    
    fs.writeFileSync(filename, JSON.stringify(debugData, null, 2));
  }

  static async profileOperation(name, operation) {
    const startTime = process.hrtime.bigint();
    const startMemory = process.memoryUsage();
    
    try {
      const result = await operation();
      
      const endTime = process.hrtime.bigint();
      const endMemory = process.memoryUsage();
      
      const duration = Number(endTime - startTime) / 1000000; // 转换为毫秒
      const memoryDiff = endMemory.heapUsed - startMemory.heapUsed;
      
      this.log('profile', `操作 ${name} 完成`, {
        duration: `${duration.toFixed(2)}ms`,
        memoryDiff: `${(memoryDiff / 1024 / 1024).toFixed(2)}MB`
      });
      
      return result;
    } catch (error) {
      this.log('error', `操作 ${name} 失败`, { error: error.message });
      throw error;
    }
  }
}
```

## 总结

本指南提供了外部 API 使用 Easy LLM CLI 模式的完整解决方案，包括：

1. **非交互模式**: 适合简单查询和批处理场景
2. **交互式进程管理**: 支持持续对话和复杂任务
3. **HTTP API 包装**: 提供标准化的 RESTful 接口
4. **WebSocket 通信**: 实现实时双向通信
5. **最佳实践**: 包括错误处理、性能优化、监控等

通过这些方法，您可以充分利用 Easy LLM CLI 的强大功能，同时保持良好的性能和稳定性。选择合适的集成方式取决于您的具体需求和应用场景。

## 相关文档

- [Easy LLM CLI 程序化 API](../programmatic-api.zh-CN.md)
- [CLI 命令参考](../cli/commands.md)
- [配置指南](../cli/configuration.md)
- [故障排除](../troubleshooting.md)
