import discord
from discord.ext import commands
from discord import ui, app_commands
import os
import random
import string
import json
import subprocess
from dotenv import load_dotenv
import asyncio
import datetime
import docker
import time
import logging
import traceback
import aiohttp
import socket
import re
import psutil
import platform
import shutil
from typing import Optional, Literal
from provisioning_policy import build_container_runtime_config, normalize_vps_credentials, validate_vps_request
import sqlite3
import pickle
import base64
import threading
from flask import Flask, render_template, request, jsonify, session
from collections import Counter
from flask_socketio import SocketIO, emit
import docker
import paramiko
import os
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hostforge_bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('HostForgeBot')

# Load environment variables
load_dotenv()

STARTUP_NOTES = []


def add_startup_note(message: str):
    STARTUP_NOTES.append(message)
    logger.info(message)


def get_env_setting(*names, default=None):
    """Read a setting from the runtime environment, falling back to .env values if available."""
    for name in names:
        value = os.getenv(name)
        if value not in (None, ""):
            return value
    return default


def is_interaction_context(ctx) -> bool:
    """Return True when the context is a Discord interaction-based command invocation."""
    if isinstance(ctx, discord.Interaction):
        return True
    if hasattr(ctx, 'interaction') and getattr(ctx, 'interaction') is not None:
        return True
    return hasattr(ctx, 'response') and hasattr(ctx, 'followup') and hasattr(ctx, 'user')


def has_vps_credentials(username: Optional[str], password: Optional[str]) -> bool:
    """Return True when both a username and password were provided for VPS creation."""
    return bool((username or "").strip()) and bool((password or "").strip())


async def send_ephemeral_message(ctx, message: str):
    """Send a message that works for both slash commands and regular command contexts."""
    if is_interaction_context(ctx):
        interaction_ctx = getattr(ctx, 'interaction', ctx)
        if hasattr(interaction_ctx, 'response') and not interaction_ctx.response.is_done():
            await interaction_ctx.response.send_message(message, ephemeral=True)
        elif hasattr(interaction_ctx, 'followup'):
            await interaction_ctx.followup.send(message, ephemeral=True)
        else:
            await ctx.send(message, ephemeral=True)
    else:
        await ctx.send(message, ephemeral=True)


# Bot configuration
TOKEN = get_env_setting('DISCORD_TOKEN', 'BOT_TOKEN')
HOST_IP = get_env_setting('HOST_IP', default=None)  # Optional, will fetch dynamically if not set
ADMIN_IDS = {int(id_) for id_ in get_env_setting('ADMIN_IDS', default='1210291131301101618').split(',') if id_.strip()}
ADMIN_ROLE_ID = int(get_env_setting('ADMIN_ROLE_ID', default='1376177459870961694'))
if not TOKEN:
    add_startup_note("DISCORD_TOKEN is not configured; the bot will fail until a token is provided.")
WATERMARK = "HostForge VPS Service"
WELCOME_MESSAGE = "Welcome To HostForge! Get Started With Us!"
MAX_VPS_PER_USER = int(get_env_setting('MAX_VPS_PER_USER', default='3'))
DEFAULT_OS_IMAGE = get_env_setting('DEFAULT_OS_IMAGE', default='ubuntu:22.04')
DOCKER_NETWORK = get_env_setting('DOCKER_NETWORK', default='bridge')
MAX_CONTAINERS = int(get_env_setting('MAX_CONTAINERS', default='100'))
DB_FILE = 'hostforge.db'
BACKUP_FILE = 'hostforge_backup.pkl'
PORT_RANGE_START = 20000
PORT_RANGE_END = 30000

# Known miner process names/patterns
MINER_PATTERNS = [
    'xmrig', 'ethminer', 'cgminer', 'sgminer', 'bfgminer',
    'minerd', 'cpuminer', 'cryptonight', 'stratum', 'pool'
]

# Dockerfile template for custom images
DOCKERFILE_TEMPLATE = """
FROM {base_image}

# Prevent prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install systemd, sudo, SSH, Docker and other essential packages
RUN apt-get update && \\
    apt-get install -y systemd systemd-sysv dbus sudo \\
                       curl gnupg2 apt-transport-https ca-certificates \\
                       software-properties-common \\
                       docker.io openssh-server tmate && \\
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Root password
RUN echo "root:root" | chpasswd

# Enable SSH login
RUN mkdir /var/run/sshd && \\
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \\
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config

# Enable services on boot
RUN systemctl enable ssh && \\
    systemctl enable docker

# HostForge customization
RUN echo '{welcome_message}' > /etc/motd && \\
    echo 'echo "{welcome_message}"' >> /root/.bashrc && \\
    echo '{watermark}' > /etc/machine-info && \\
    echo 'hostforge-{vps_id}' > /etc/hostname

# Install additional useful packages
RUN apt-get update && \\
    apt-get install -y neofetch htop nano vim wget git tmux net-tools dnsutils iputils-ping && \\
    apt-get clean && \\
    rm -rf /var/lib/apt/lists/*

# Fix systemd inside container
STOPSIGNAL SIGRTMIN+3

# Boot into systemd (like a VM)
CMD ["/sbin/init"]
"""


def humanize_bytes(size_bytes: int) -> str:
    """Format byte counts into a human-readable size string."""
    if size_bytes < 1024:
        return f"{size_bytes} B"
    size_kb = size_bytes / 1024
    if size_kb < 1024:
        return f"{size_kb:.1f} KB"
    size_mb = size_kb / 1024
    if size_mb < 1024:
        return f"{size_mb:.1f} MB"
    size_gb = size_mb / 1024
    return f"{size_gb:.1f} GB"


class Database:
    """Handles all data persistence using SQLite3"""
    def __init__(self, db_file):
        self.conn = sqlite3.connect(db_file, check_same_thread=False)
        self.cursor = self.conn.cursor()
        self._create_tables()
        self._initialize_settings()

    def _create_tables(self):
        """Create necessary tables"""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS vps_instances (
                token TEXT PRIMARY KEY,
                vps_id TEXT UNIQUE,
                container_id TEXT,
                memory INTEGER,
                cpu INTEGER,
                disk INTEGER,
                username TEXT,
                password TEXT,
                root_password TEXT,
                created_by TEXT,
                created_at TEXT,
                tmate_session TEXT,
                watermark TEXT,
                os_image TEXT,
                restart_count INTEGER DEFAULT 0,
                last_restart TEXT,
                status TEXT DEFAULT 'running',
                use_custom_image BOOLEAN DEFAULT 1,
                external_ssh_port INTEGER
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS usage_stats (
                key TEXT PRIMARY KEY,
                value INTEGER DEFAULT 0
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id TEXT PRIMARY KEY
            )
        ''')
        
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS admin_users (
                user_id TEXT PRIMARY KEY
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS activity_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                actor TEXT,
                target TEXT,
                detail TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                vps_id TEXT NOT NULL,
                path TEXT NOT NULL,
                created_by TEXT,
                size_bytes INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_quotas (
                user_id TEXT PRIMARY KEY,
                quota INTEGER NOT NULL DEFAULT 0
            )
        ''')
        
        self.conn.commit()

    def _initialize_settings(self):
        """Initialize default settings"""
        defaults = {
            'max_containers': str(MAX_CONTAINERS),
            'max_vps_per_user': str(MAX_VPS_PER_USER)
        }
        for key, value in defaults.items():
            self.cursor.execute('INSERT OR IGNORE INTO system_settings (key, value) VALUES (?, ?)', (key, value))
        
        # Load admin users from database
        self.cursor.execute('SELECT user_id FROM admin_users')
        for row in self.cursor.fetchall():
            ADMIN_IDS.add(int(row[0]))

        self.cursor.execute('SELECT value FROM system_settings WHERE key = ?', ('auto_cleanup_enabled',))
        if self.cursor.fetchone() is None:
            self.cursor.execute('INSERT INTO system_settings (key, value) VALUES (?, ?)', ('auto_cleanup_enabled', '1'))
            
        self.conn.commit()

    def log_activity(self, action, actor=None, target=None, detail=None):
        self.cursor.execute(
            'INSERT INTO activity_log (action, actor, target, detail) VALUES (?, ?, ?, ?)',
            (action, str(actor) if actor is not None else None, target, detail)
        )
        self.conn.commit()

    def get_recent_activity(self, limit=10):
        self.cursor.execute(
            'SELECT action, actor, target, detail, created_at FROM activity_log ORDER BY id DESC LIMIT ?',
            (limit,)
        )
        rows = self.cursor.fetchall()
        return [
            {
                'action': row[0],
                'actor': row[1],
                'target': row[2],
                'detail': row[3],
                'created_at': row[4],
            }
            for row in rows
        ]

    def set_user_quota(self, user_id, quota):
        self.cursor.execute('INSERT OR REPLACE INTO user_quotas (user_id, quota) VALUES (?, ?)', (str(user_id), int(quota)))
        self.conn.commit()

    def get_user_quota(self, user_id):
        self.cursor.execute('SELECT quota FROM user_quotas WHERE user_id = ?', (str(user_id),))
        result = self.cursor.fetchone()
        return int(result[0]) if result else None

    def get_quota_limit(self, user_id, default=None):
        quota = self.get_user_quota(user_id)
        return quota if quota is not None else default

    def add_snapshot(self, vps_id, snapshot_id, path, created_by=None, size_bytes=0):
        self.cursor.execute(
            'INSERT OR REPLACE INTO snapshots (snapshot_id, vps_id, path, created_by, size_bytes) VALUES (?, ?, ?, ?, ?)',
            (snapshot_id, vps_id, path, str(created_by) if created_by is not None else None, int(size_bytes))
        )
        self.conn.commit()

    def list_snapshots(self, vps_id):
        self.cursor.execute('SELECT snapshot_id, path, created_by, size_bytes, created_at FROM snapshots WHERE vps_id = ? ORDER BY created_at DESC', (vps_id,))
        rows = self.cursor.fetchall()
        return [
            {
                'snapshot_id': row[0],
                'path': row[1],
                'created_by': row[2],
                'size_bytes': row[3],
                'created_at': row[4],
            }
            for row in rows
        ]

    def delete_snapshot(self, snapshot_id):
        self.cursor.execute('DELETE FROM snapshots WHERE snapshot_id = ?', (snapshot_id,))
        self.conn.commit()
        return self.cursor.rowcount > 0

    def get_setting(self, key, default=None):
        self.cursor.execute('SELECT value FROM system_settings WHERE key = ?', (key,))
        result = self.cursor.fetchone()
        return int(result[0]) if result else default

    def set_setting(self, key, value):
        self.cursor.execute('INSERT OR REPLACE INTO system_settings (key, value) VALUES (?, ?)', (key, str(value)))
        self.conn.commit()

    def get_stat(self, key, default=0):
        self.cursor.execute('SELECT value FROM usage_stats WHERE key = ?', (key,))
        result = self.cursor.fetchone()
        return result[0] if result else default

    def increment_stat(self, key, amount=1):
        current = self.get_stat(key)
        self.cursor.execute('INSERT OR REPLACE INTO usage_stats (key, value) VALUES (?, ?)', (key, current + amount))
        self.conn.commit()

    def get_vps_by_id(self, vps_id):
        self.cursor.execute('SELECT * FROM vps_instances WHERE vps_id = ?', (vps_id,))
        row = self.cursor.fetchone()
        if not row:
            return None, None
        columns = [desc[0] for desc in self.cursor.description]
        vps = dict(zip(columns, row))
        return vps['token'], vps

    def get_vps_by_token(self, token):
        self.cursor.execute('SELECT * FROM vps_instances WHERE token = ?', (token,))
        row = self.cursor.fetchone()
        if not row:
            return None
        columns = [desc[0] for desc in self.cursor.description]
        return dict(zip(columns, row))

    def get_user_vps_count(self, user_id):
        self.cursor.execute('SELECT COUNT(*) FROM vps_instances WHERE created_by = ?', (str(user_id),))
        return self.cursor.fetchone()[0]

    def get_user_vps(self, user_id):
        self.cursor.execute('SELECT * FROM vps_instances WHERE created_by = ?', (str(user_id),))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def get_all_vps(self):
        self.cursor.execute('SELECT * FROM vps_instances')
        columns = [desc[0] for desc in self.cursor.description]
        return {row[0]: dict(zip(columns, row)) for row in self.cursor.fetchall()}

    def add_vps(self, vps_data):
        columns = ', '.join(vps_data.keys())
        placeholders = ', '.join('?' for _ in vps_data)
        self.cursor.execute(f'INSERT INTO vps_instances ({columns}) VALUES ({placeholders})', tuple(vps_data.values()))
        self.conn.commit()
        self.increment_stat('total_vps_created')

    def remove_vps(self, token):
        self.cursor.execute('DELETE FROM vps_instances WHERE token = ?', (token,))
        self.conn.commit()
        return self.cursor.rowcount > 0

    def update_vps(self, token, updates):
        set_clause = ', '.join(f'{k} = ?' for k in updates)
        values = list(updates.values()) + [token]
        self.cursor.execute(f'UPDATE vps_instances SET {set_clause} WHERE token = ?', values)
        self.conn.commit()
        return self.cursor.rowcount > 0

    def is_user_banned(self, user_id):
        self.cursor.execute('SELECT 1 FROM banned_users WHERE user_id = ?', (str(user_id),))
        return self.cursor.fetchone() is not None

    def ban_user(self, user_id):
        self.cursor.execute('INSERT OR IGNORE INTO banned_users (user_id) VALUES (?)', (str(user_id),))
        self.conn.commit()

    def unban_user(self, user_id):
        self.cursor.execute('DELETE FROM banned_users WHERE user_id = ?', (str(user_id),))
        self.conn.commit()

    def get_banned_users(self):
        self.cursor.execute('SELECT user_id FROM banned_users')
        return [row[0] for row in self.cursor.fetchall()]

    def add_admin(self, user_id):
        self.cursor.execute('INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)', (str(user_id),))
        self.conn.commit()
        ADMIN_IDS.add(int(user_id))

    def remove_admin(self, user_id):
        self.cursor.execute('DELETE FROM admin_users WHERE user_id = ?', (str(user_id),))
        self.conn.commit()
        if int(user_id) in ADMIN_IDS:
            ADMIN_IDS.remove(int(user_id))

    def get_admins(self):
        self.cursor.execute('SELECT user_id FROM admin_users')
        return [row[0] for row in self.cursor.fetchall()]

    def get_used_ports(self):
        self.cursor.execute('SELECT external_ssh_port FROM vps_instances WHERE external_ssh_port IS NOT NULL')
        return {row[0] for row in self.cursor.fetchall()}

    def backup_data(self):
        """Backup all data to a file"""
        data = {
            'vps_instances': self.get_all_vps(),
            'usage_stats': {},
            'system_settings': {},
            'banned_users': self.get_banned_users(),
            'admin_users': self.get_admins()
        }
        
        # Get usage stats
        self.cursor.execute('SELECT * FROM usage_stats')
        for row in self.cursor.fetchall():
            data['usage_stats'][row[0]] = row[1]
            
        # Get system settings
        self.cursor.execute('SELECT * FROM system_settings')
        for row in self.cursor.fetchall():
            data['system_settings'][row[0]] = row[1]
            
        with open(BACKUP_FILE, 'wb') as f:
            pickle.dump(data, f)
            
        return True

    def restore_data(self):
        """Restore data from backup file"""
        if not os.path.exists(BACKUP_FILE):
            return False
            
        try:
            with open(BACKUP_FILE, 'rb') as f:
                data = pickle.load(f)
                
            # Clear all tables
            self.cursor.execute('DELETE FROM vps_instances')
            self.cursor.execute('DELETE FROM usage_stats')
            self.cursor.execute('DELETE FROM system_settings')
            self.cursor.execute('DELETE FROM banned_users')
            self.cursor.execute('DELETE FROM admin_users')
            
            # Restore VPS instances
            for token, vps in data['vps_instances'].items():
                columns = ', '.join(vps.keys())
                placeholders = ', '.join('?' for _ in vps)
                self.cursor.execute(f'INSERT INTO vps_instances ({columns}) VALUES ({placeholders})', tuple(vps.values()))
            
            # Restore usage stats
            for key, value in data['usage_stats'].items():
                self.cursor.execute('INSERT INTO usage_stats (key, value) VALUES (?, ?)', (key, value))
                
            # Restore system settings
            for key, value in data['system_settings'].items():
                self.cursor.execute('INSERT INTO system_settings (key, value) VALUES (?, ?)', (key, value))
                
            # Restore banned users
            for user_id in data['banned_users']:
                self.cursor.execute('INSERT INTO banned_users (user_id) VALUES (?)', (user_id,))
                
            # Restore admin users
            for user_id in data['admin_users']:
                self.cursor.execute('INSERT INTO admin_users (user_id) VALUES (?)', (user_id,))
                ADMIN_IDS.add(int(user_id))
                
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"Error restoring data: {e}")
            return False

    def close(self):
        self.conn.close()

# Initialize bot with command prefix '/'
class HostForgeBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = Database(DB_FILE)
        self.session = None
        self.docker_client = None
        self.public_ip = None
        self.system_stats = {
            'cpu_usage': 0,
            'memory_usage': 0,
            'disk_usage': 0,
            'network_io': (0, 0),
            'last_updated': 0
        }
        self.my_persistent_views = {}

    async def setup_hook(self):
        self.session = aiohttp.ClientSession()
        try:
            self.docker_client = docker.from_env()
            add_startup_note("Docker client initialized successfully")
            self.public_ip = HOST_IP or await self.get_public_ip()
            add_startup_note(f"Public IP: {self.public_ip}")
            self.loop.create_task(self.update_system_stats())
            self.loop.create_task(self.anti_miner_monitor())
            await self.reconnect_containers()
            await self.restore_persistent_views()
        except Exception as e:
            logger.error(f"Failed to initialize Docker client: {e}")
            self.docker_client = None
            add_startup_note("Docker integration is unavailable; VPS operations will be limited.")

    async def get_public_ip(self):
        try:
            async with self.session.get('https://api.ipify.org') as resp:
                if resp.status == 200:
                    return await resp.text()
                else:
                    logger.error(f"Failed to get public IP: {resp.status}")
                    return '127.0.0.1'  # Fallback
        except Exception as e:
            logger.error(f"Error getting public IP: {e}")
            return '127.0.0.1'

    async def reconnect_containers(self):
        """Reconnect to existing containers on startup"""
        if not self.docker_client:
            return
            
        for token, vps in list(self.db.get_all_vps().items()):
            if vps['status'] == 'running':
                try:
                    container = self.docker_client.containers.get(vps['container_id'])
                    if container.status != 'running':
                        container.start()
                    logger.info(f"Reconnected and started container for VPS {vps['vps_id']}")
                except docker.errors.NotFound:
                    logger.warning(f"Container {vps['container_id']} not found, removing from data")
                    self.db.remove_vps(token)
                except Exception as e:
                    logger.error(f"Error reconnecting container {vps['vps_id']}: {e}")

    async def restore_persistent_views(self):
        """Restore persistent views after restart"""
        # This would be implemented to restore any persistent UI components
        pass

    async def anti_miner_monitor(self):
        """Periodically check for mining activities"""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                for token, vps in self.db.get_all_vps().items():
                    if vps['status'] != 'running':
                        continue
                    try:
                        container = self.docker_client.containers.get(vps['container_id'])
                        if container.status != 'running':
                            continue
                        
                        # Check processes
                        exec_result = container.exec_run("ps aux")
                        output = exec_result.output.decode().lower()
                        
                        for pattern in MINER_PATTERNS:
                            if pattern in output:
                                logger.warning(f"Mining detected in VPS {vps['vps_id']}, suspending...")
                                container.stop()
                                self.db.update_vps(token, {'status': 'suspended'})
                                # Notify owner
                                try:
                                    owner = await self.fetch_user(int(vps['created_by']))
                                    await owner.send(f"⚠️ Your VPS {vps['vps_id']} has been suspended due to detected mining activity. Contact admin to unsuspend.")
                                except:
                                    pass
                                break
                    except Exception as e:
                        logger.error(f"Error checking VPS {vps['vps_id']} for mining: {e}")
            except Exception as e:
                logger.error(f"Error in anti_miner_monitor: {e}")
            await asyncio.sleep(300)  # Check every 5 minutes

    async def update_system_stats(self):
        """Update system statistics periodically"""
        await self.wait_until_ready()
        while not self.is_closed():
            try:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                
                # Memory usage
                mem = psutil.virtual_memory()
                
                # Disk usage
                disk = psutil.disk_usage('/')
                
                # Network IO
                net_io = psutil.net_io_counters()
                
                self.system_stats = {
                    'cpu_usage': cpu_percent,
                    'memory_usage': mem.percent,
                    'memory_used': mem.used / (1024 ** 3),  # GB
                    'memory_total': mem.total / (1024 ** 3),  # GB
                    'disk_usage': disk.percent,
                    'disk_used': disk.used / (1024 ** 3),  # GB
                    'disk_total': disk.total / (1024 ** 3),  # GB
                    'network_sent': net_io.bytes_sent / (1024 ** 2),  # MB
                    'network_recv': net_io.bytes_recv / (1024 ** 2),  # MB
                    'last_updated': time.time()
                }
            except Exception as e:
                logger.error(f"Error updating system stats: {e}")
            await asyncio.sleep(30)

    async def close(self):
        await super().close()
        if self.session:
            await self.session.close()
        if self.docker_client:
            self.docker_client.close()
        self.db.close()

def generate_token():
    """Generate a random token for VPS access"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=24))

def generate_vps_id():
    """Generate a unique VPS ID"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

def get_available_port(db):
    """Get an available random port for SSH forwarding"""
    used_ports = db.get_used_ports()
    while True:
        port = random.randint(PORT_RANGE_START, PORT_RANGE_END)
        if port not in used_ports:
            return port


def generate_ssh_password(length: int = 16):
    """Generate a strong random SSH password."""
    alphabet = string.ascii_letters + string.digits
    return ''.join(random.choice(alphabet) for _ in range(length))


def should_use_simulated_vps(docker_client) -> bool:
    """Return True when the bot should run in a non-Docker fallback mode."""
    return docker_client is None

async def capture_ssh_session_line(process):
    """Capture the SSH session line from tmate output"""
    try:
        while True:
            output = await process.stdout.readline()
            if not output:
                break
            output = output.decode('utf-8').strip()
            if "ssh session:" in output:
                return output.split("ssh session:")[1].strip()
        return None
    except Exception as e:
        logger.error(f"Error capturing SSH session: {e}")
        return None

async def run_docker_command(container_id, command, timeout=120):
    """Run a Docker command asynchronously with timeout"""
    try:
        process = await asyncio.create_subprocess_exec(
            "docker", "exec", container_id, *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            if process.returncode != 0:
                raise Exception(f"Command failed: {stderr.decode()}")
            return True, stdout.decode()
        except asyncio.TimeoutError:
            process.kill()
            raise Exception(f"Command timed out after {timeout} seconds")
    except Exception as e:
        logger.error(f"Error running Docker command: {e}")
        return False, str(e)

async def kill_apt_processes(container_id):
    """Kill any running apt processes"""
    try:
        success, _ = await run_docker_command(container_id, ["bash", "-c", "killall apt apt-get dpkg || true"])
        await asyncio.sleep(2)
        success, _ = await run_docker_command(container_id, ["bash", "-c", "rm -f /var/lib/apt/lists/lock /var/cache/apt/archives/lock /var/lib/dpkg/lock*"])
        await asyncio.sleep(2)
        return success
    except Exception as e:
        logger.error(f"Error killing apt processes: {e}")
        return False

async def wait_for_apt_lock(container_id, status_msg):
    """Wait for apt lock to be released"""
    max_attempts = 5
    for attempt in range(max_attempts):
        try:
            await kill_apt_processes(container_id)
            
            process = await asyncio.create_subprocess_exec(
                "docker", "exec", container_id, "bash", "-c", "lsof /var/lib/dpkg/lock-frontend",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                return True
                
            if isinstance(status_msg, discord.Interaction):
                await status_msg.followup.send(f"🔄 Waiting for package manager to be ready... (Attempt {attempt + 1}/{max_attempts})", ephemeral=True)
            else:
                await status_msg.edit(content=f"🔄 Waiting for package manager to be ready... (Attempt {attempt + 1}/{max_attempts})")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"Error checking apt lock: {e}")
            await asyncio.sleep(5)
    
    return False

async def build_custom_image(vps_id, base_image=DEFAULT_OS_IMAGE):
    """Build a custom Docker image using our template"""
    try:
        # Create a temporary directory for the Dockerfile
        temp_dir = f"temp_dockerfiles/{vps_id}"
        os.makedirs(temp_dir, exist_ok=True)
        
        # Generate Dockerfile content
        dockerfile_content = DOCKERFILE_TEMPLATE.format(
            base_image=base_image,
            welcome_message=WELCOME_MESSAGE,
            watermark=WATERMARK,
            vps_id=vps_id
        )
        
        # Write Dockerfile
        dockerfile_path = os.path.join(temp_dir, "Dockerfile")
        with open(dockerfile_path, 'w') as f:
            f.write(dockerfile_content)
        
        # Build the image
        image_tag = f"hostforge/{vps_id.lower()}:latest"
        build_process = await asyncio.create_subprocess_exec(
            "docker", "build", "-t", image_tag, temp_dir,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await build_process.communicate()
        
        if build_process.returncode != 0:
            raise Exception(f"Failed to build image: {stderr.decode()}")
        
        return image_tag
    except Exception as e:
        logger.error(f"Error building custom image: {e}")
        raise
    finally:
        # Clean up temporary directory
        try:
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
        except Exception as e:
            logger.error(f"Error cleaning up temp directory: {e}")

async def setup_container(container_id, status_msg, memory, vps_id=None, use_custom_image=False):
    """Enhanced container setup with HostForge customization"""
    try:
        # Ensure container is running
        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send("🔍 Checking container status...", ephemeral=True)
        else:
            await status_msg.edit(content="🔍 Checking container status...")
            
        container = bot.docker_client.containers.get(container_id)
        if container.status != "running":
            if isinstance(status_msg, discord.Interaction):
                await status_msg.followup.send("🚀 Starting container...", ephemeral=True)
            else:
                await status_msg.edit(content="🚀 Starting container...")
            container.start()
            await asyncio.sleep(5)

        # Install tmate and other required packages if not custom
        if not use_custom_image:
            if isinstance(status_msg, discord.Interaction):
                await status_msg.followup.send("📦 Installing required packages...", ephemeral=True)
            else:
                await status_msg.edit(content="📦 Installing required packages...")
                
            # Update package list
            success, output = await run_docker_command(container_id, ["apt-get", "update"])
            if not success:
                raise Exception(f"Failed to update package list: {output}")

            # Install packages
            packages = [
                "tmate", "neofetch", "screen", "wget", "curl", "htop", "nano", "vim", 
                "openssh-server", "sudo", "ufw", "git", "docker.io", "systemd", "systemd-sysv"
            ]
            success, output = await run_docker_command(container_id, ["apt-get", "install", "-y"] + packages)
            if not success:
                raise Exception(f"Failed to install packages: {output}")

        # Setup SSH
        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send("🔐 Configuring SSH access...", ephemeral=True)
        else:
            await status_msg.edit(content="🔐 Configuring SSH access...")
            
        # For non-custom image, setup root
        if not use_custom_image:
            setup_commands = [
                'echo "root:root" | chpasswd',
                "sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config",
                "sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config",
                "service ssh restart"
            ]
            
            for cmd in setup_commands:
                success, output = await run_docker_command(container_id, ["bash", "-c", cmd])
                if not success:
                    raise Exception(f"Failed to setup SSH: {output}")

        # Set HostForge customization
        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send("🎨 Setting up HostForge customization...", ephemeral=True)
        else:
            await status_msg.edit(content="🎨 Setting up HostForge customization...")
            
        # Create welcome message file
        welcome_cmd = f"echo '{WELCOME_MESSAGE}' > /etc/motd && echo 'echo \"{WELCOME_MESSAGE}\"' >> /root/.bashrc"
        success, output = await run_docker_command(container_id, ["bash", "-c", welcome_cmd])
        if not success:
            logger.warning(f"Could not set welcome message: {output}")

        # Set hostname and watermark
        if not vps_id:
            vps_id = generate_vps_id()
        hostname_cmd = f"echo 'hostforge-{vps_id}' > /etc/hostname && hostname hostforge-{vps_id}"
        success, output = await run_docker_command(container_id, ["bash", "-c", hostname_cmd])
        if not success:
            raise Exception(f"Failed to set hostname: {output}")

        # Set memory limit in cgroup
        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send("⚙️ Setting resource limits...", ephemeral=True)
        else:
            await status_msg.edit(content="⚙️ Setting resource limits...")
            
        memory_bytes = memory * 1024 * 1024 * 1024
        success, output = await run_docker_command(container_id, ["bash", "-c", f"echo {memory_bytes} > /sys/fs/cgroup/memory.max"])
        if not success:
            logger.warning(f"Could not set memory limit in cgroup: {output}")

        # Set watermark in machine info
        success, output = await run_docker_command(container_id, ["bash", "-c", f"echo '{WATERMARK}' > /etc/machine-info"])
        if not success:
            logger.warning(f"Could not set machine info: {output}")

        # Basic security setup
        security_commands = [
            "ufw allow ssh",
            "ufw --force enable",
            "apt-get -y autoremove",
            "apt-get clean"
        ]
        
        for cmd in security_commands:
            success, output = await run_docker_command(container_id, ["bash", "-c", cmd])
            if not success:
                logger.warning(f"Security setup command failed: {cmd} - {output}")

        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send("✅ HostForge VPS setup completed successfully!", ephemeral=True)
        else:
            await status_msg.edit(content="✅ HostForge VPS setup completed successfully!")
            
        return True, vps_id
    except Exception as e:
        error_msg = f"Setup failed: {str(e)}"
        logger.error(error_msg)
        if isinstance(status_msg, discord.Interaction):
            await status_msg.followup.send(f"❌ {error_msg}", ephemeral=True)
        else:
            await status_msg.edit(content=f"❌ {error_msg}")
        return False, None

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = HostForgeBot(command_prefix='/', intents=intents, help_command=None)

@bot.event
async def on_ready():
    logger.info(f'{bot.user} has connected to Discord!')
    
    # Auto-start VPS containers based on status
    if bot.docker_client:
        for token, vps in bot.db.get_all_vps().items():
            if vps['status'] == 'running':
                try:
                    container = bot.docker_client.containers.get(vps["container_id"])
                    if container.status != "running":
                        container.start()
                        logger.info(f"Started container for VPS {vps['vps_id']}")
                except docker.errors.NotFound:
                    logger.warning(f"Container {vps['container_id']} not found")
                except Exception as e:
                    logger.error(f"Error starting container: {e}")
    
    try:
        await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="HostForge VPS"))
        synced_commands = await bot.tree.sync()
        logger.info(f"Synced {len(synced_commands)} slash commands")
    except Exception as e:
        logger.error(f"Error syncing slash commands: {e}")

@bot.hybrid_command(name='health', description='Show bot health and deployment status')
async def health_check(ctx):
    """Show bot health and configuration status."""
    try:
        docker_status = "available" if bot.docker_client else "unavailable"
        container_count = len(bot.db.get_all_vps())
        running_count = sum(1 for vps in bot.db.get_all_vps().values() if vps.get('status') == 'running')
        embed = discord.Embed(title="Bot Health", color=discord.Color.green())
        embed.add_field(name="Discord Token", value="configured" if TOKEN else "missing", inline=True)
        embed.add_field(name="Docker", value=docker_status, inline=True)
        embed.add_field(name="Public IP", value=bot.public_ip or "unknown", inline=True)
        embed.add_field(name="VPS Records", value=f"{container_count} total / {running_count} running", inline=True)
        if STARTUP_NOTES:
            embed.add_field(name="Startup Notes", value="\n".join(STARTUP_NOTES[-5:]), inline=False)
        await ctx.send(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in health_check: {e}")
        await ctx.send("❌ Error fetching health info.", ephemeral=True)


@bot.hybrid_command(name='backup_report', description='Show backup and storage status (Admin only)')
async def backup_report(ctx):
    """Show backup-related status and storage info."""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        data_exists = os.path.exists(BACKUP_FILE)
        backup_age = None
        if data_exists:
            backup_age = time.time() - os.path.getmtime(BACKUP_FILE)
        embed = discord.Embed(title="Backup Report", color=discord.Color.blue())
        embed.add_field(name="Backup File", value="present" if data_exists else "missing", inline=True)
        if backup_age is not None:
            embed.add_field(name="Backup Age", value=f"{int(backup_age // 60)} minutes", inline=True)
        embed.add_field(name="Database File", value="present" if os.path.exists(DB_FILE) else "missing", inline=True)
        embed.add_field(name="Auto Cleanup", value="enabled" if bot.db.get_setting('auto_cleanup_enabled', 1) else "disabled", inline=True)
        await ctx.send(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in backup_report: {e}")
        await ctx.send("❌ Error fetching backup report.", ephemeral=True)


@bot.hybrid_command(name='toggle_auto_cleanup', description='Enable or disable automatic cleanup (Admin only)')
async def toggle_auto_cleanup(ctx):
    """Toggle automatic cleanup for inactive VPS instances."""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    enabled = bot.db.get_setting('auto_cleanup_enabled', 1)
    bot.db.set_setting('auto_cleanup_enabled', 0 if enabled else 1)
    await ctx.send(f"✅ Auto cleanup {'disabled' if not enabled else 'enabled'}", ephemeral=True)


@bot.hybrid_command(name='activity_log', description='Show recent bot activity (Admin only)')
async def activity_log(ctx):
    """Show recent bot activity for admin auditing."""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        logs = bot.db.get_recent_activity(limit=10)
        if not logs:
            await ctx.send("ℹ️ No activity recorded yet.", ephemeral=True)
            return

        lines = []
        for entry in logs:
            lines.append(f"{entry['created_at']} | {entry['action']} | target={entry['target'] or 'n/a'} | actor={entry['actor'] or 'system'}")
        embed = discord.Embed(title="Recent Activity", color=discord.Color.gold())
        embed.description = "\n".join(lines)
        await ctx.send(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in activity_log: {e}")
        await ctx.send("❌ Error fetching activity log.", ephemeral=True)


@bot.hybrid_command(name='set_quota', description='Set a user VPS quota (Admin only)')
@app_commands.describe(
    user="User to set a quota for",
    quota="Maximum number of VPS instances"
)
async def set_quota(ctx, user: discord.User, quota: int):
    """Set the maximum number of VPS instances a user may create."""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    if quota < 0:
        await ctx.send("❌ Quota must be zero or greater.", ephemeral=True)
        return

    bot.db.set_user_quota(user.id, quota)
    await ctx.send(f"✅ Quota for {user.mention} set to {quota}", ephemeral=True)


@bot.hybrid_command(name='quota_status', description='Show your current VPS quota')
async def quota_status(ctx):
    """Show the current quota and usage for the requesting user."""
    try:
        quota_limit = bot.db.get_quota_limit(ctx.author.id, default=bot.db.get_setting('max_vps_per_user', MAX_VPS_PER_USER))
        used = bot.db.get_user_vps_count(ctx.author.id)
        embed = discord.Embed(title="VPS Quota", color=discord.Color.teal())
        embed.add_field(name="Used", value=used, inline=True)
        embed.add_field(name="Limit", value=quota_limit, inline=True)
        embed.add_field(name="Remaining", value=max(quota_limit - used, 0), inline=True)
        await ctx.send(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in quota_status: {e}")
        await ctx.send("❌ Error fetching quota status.", ephemeral=True)


@bot.hybrid_command(name='create_snapshot', description='Create a snapshot of a VPS')
@app_commands.describe(
    vps_id="ID of the VPS to snapshot"
)
async def create_snapshot(ctx, vps_id: str):
    """Create a snapshot archive for a VPS."""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps['created_by'] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        if not bot.docker_client:
            await ctx.send("❌ Docker is unavailable.", ephemeral=True)
            return

        snapshot_id = generate_vps_id()[:8]
        snapshot_dir = os.path.join('snapshots', vps_id)
        os.makedirs(snapshot_dir, exist_ok=True)
        snapshot_path = os.path.join(snapshot_dir, f"{snapshot_id}.tar")
        await ctx.send(f"🔄 Creating snapshot {snapshot_id} for VPS {vps_id}...")
        result = subprocess.run(["docker", "export", "-o", snapshot_path, vps['container_id']], capture_output=True, text=True)
        if result.returncode != 0:
            raise Exception(result.stderr.strip() or result.stdout.strip())

        size_bytes = os.path.getsize(snapshot_path) if os.path.exists(snapshot_path) else 0
        bot.db.add_snapshot(vps_id, snapshot_id, snapshot_path, created_by=vps['created_by'], size_bytes=size_bytes)
        bot.db.log_activity('create_snapshot', actor=str(ctx.author.id), target=vps_id, detail=snapshot_id)
        await ctx.send(f"✅ Snapshot created: {snapshot_id} ({humanize_bytes(size_bytes)})", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in create_snapshot: {e}")
        await ctx.send(f"❌ Error creating snapshot: {str(e)}", ephemeral=True)


@bot.hybrid_command(name='list_snapshots', description='List snapshots for a VPS')
@app_commands.describe(
    vps_id="ID of the VPS"
)
async def list_snapshots(ctx, vps_id: str):
    """List snapshots for a VPS."""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps['created_by'] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        snapshots = bot.db.list_snapshots(vps_id)
        if not snapshots:
            await ctx.send("ℹ️ No snapshots found for this VPS.", ephemeral=True)
            return

        embed = discord.Embed(title=f"Snapshots for VPS {vps_id}", color=discord.Color.blue())
        for snapshot in snapshots:
            embed.add_field(
                name=snapshot['snapshot_id'],
                value=f"{humanize_bytes(snapshot['size_bytes'])} | {snapshot['created_at']}",
                inline=False
            )
        await ctx.send(embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"Error in list_snapshots: {e}")
        await ctx.send("❌ Error listing snapshots.", ephemeral=True)


@bot.hybrid_command(name='delete_snapshot', description='Delete a VPS snapshot')
@app_commands.describe(
    snapshot_id="ID of the snapshot"
)
async def delete_snapshot(ctx, snapshot_id: str):
    """Delete a snapshot from the database and disk."""
    try:
        snapshots = bot.db.list_snapshots('')
        snapshot_entry = None
        for entry in bot.db.get_all_vps().values():
            pass
        # The snapshot lookup is done by scanning the database directly for the snapshot id.
        # This keeps the feature simple and robust for the current storage model.
        from sqlite3 import connect
        with connect(DB_FILE) as temp_conn:
            temp_cursor = temp_conn.cursor()
            temp_cursor.execute('SELECT vps_id, path FROM snapshots WHERE snapshot_id = ?', (snapshot_id,))
            row = temp_cursor.fetchone()
            if row:
                snapshot_entry = {'vps_id': row[0], 'path': row[1]}

        if not snapshot_entry:
            await ctx.send("❌ Snapshot not found.", ephemeral=True)
            return

        if os.path.exists(snapshot_entry['path']):
            os.remove(snapshot_entry['path'])
        deleted = bot.db.delete_snapshot(snapshot_id)
        if deleted:
            bot.db.log_activity('delete_snapshot', actor=str(ctx.author.id), target=snapshot_id, detail='removed')
            await ctx.send(f"✅ Snapshot {snapshot_id} deleted.", ephemeral=True)
        else:
            await ctx.send("❌ Snapshot could not be deleted.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in delete_snapshot: {e}")
        await ctx.send("❌ Error deleting snapshot.", ephemeral=True)


@bot.hybrid_command(name='help', description='Show all available commands')
async def show_commands(ctx):
    """Show all available commands"""
    try:
        embed = discord.Embed(title="🤖 HostForge VPS Bot Commands", color=discord.Color.blue())
        
        # User commands
        embed.add_field(name="User Commands", value="""
`/create_vps` - Create a new VPS (Admin only)
`/connect_vps <token>` - Connect to your VPS
`/list` - List all your VPS instances
`/help` - Show this help message
`/manage_vps <vps_id>` - Manage your VPS
`/transfer_vps <vps_id> <user>` - Transfer VPS ownership
`/vps_stats <vps_id>` - Show VPS resource usage
`/change_ssh_password <vps_id>` - Change SSH password
`/vps_shell <vps_id>` - Get shell access to your VPS
`/vps_console <vps_id>` - Get direct console access to your VPS
`/vps_usage` - Show your VPS usage statistics
`/run_command <vps_id> <command>` - Run a command in your VPS
`/quota_status` - Show your current VPS quota
`/create_snapshot <vps_id>` - Create a VPS snapshot
`/list_snapshots <vps_id>` - List VPS snapshots
`/delete_snapshot <snapshot_id>` - Delete a VPS snapshot
""", inline=False)
        
        # Admin commands
        if has_admin_role(ctx):
            embed.add_field(name="Admin Commands", value="""
`/vps_list` - List all VPS instances
`/delete_vps <vps_id>` - Delete a VPS
`/admin_stats` - Show system statistics
`/cleanup_vps` - Cleanup inactive VPS instances
`/add_admin <user>` - Add a new admin
`/remove_admin <user>` - Remove an admin (Owner only)
`/list_admins` - List all admin users
`/system_info` - Show detailed system information
`/container_limit <max>` - Set maximum container limit
`/global_stats` - Show global usage statistics
`/migrate_vps <vps_id>` - Migrate VPS to another host
`/emergency_stop <vps_id>` - Force stop a problematic VPS
`/emergency_remove <vps_id>` - Force remove a problematic VPS
`/suspend_vps <vps_id>` - Suspend a VPS
`/unsuspend_vps <vps_id>` - Unsuspend a VPS
`/edit_vps <vps_id> <memory> <cpu> <disk>` - Edit VPS specifications
`/ban_user <user>` - Ban a user from creating VPS
`/unban_user <user>` - Unban a user
`/list_banned` - List banned users
`/backup_data` - Backup all data
`/restore_data` - Restore from backup
`/activity_log` - Show recent bot activity
`/set_quota <user> <quota>` - Set a user VPS quota
`/toggle_auto_cleanup` - Enable or disable automatic cleanup
`/reinstall_bot` - Reinstall the bot (Owner only)
""", inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in show_commands: {e}")
        await ctx.send("❌ An error occurred while processing your request.")

def has_admin_role(ctx):
    """Check if user has admin role or is in ADMIN_IDS"""
    if isinstance(ctx, discord.Interaction):
        user_id = ctx.user.id
        roles = ctx.user.roles
    else:
        user_id = ctx.author.id
        roles = ctx.author.roles
    
    if user_id in ADMIN_IDS:
        return True
    
    return any(role.id == ADMIN_ROLE_ID for role in roles)

@bot.hybrid_command(name='add_admin', description='Add a new admin (Admin only)')
@app_commands.describe(
    user="User to make admin"
)
async def add_admin(ctx, user: discord.User):
    """Add a new admin user"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return
    
    bot.db.add_admin(user.id)
    await ctx.send(f"✅ {user.mention} has been added as an admin!", ephemeral=True)

@bot.hybrid_command(name='remove_admin', description='Remove an admin (Owner only)')
@app_commands.describe(
    user="User to remove from admin"
)
async def remove_admin(ctx, user: discord.User):
    """Remove an admin user (Owner only)"""
    if ctx.author.id != 1210291131301101618:  # Only the owner can remove admins
        await ctx.send("❌ Only the owner can remove admins!", ephemeral=True)
        return
    
    bot.db.remove_admin(user.id)
    await ctx.send(f"✅ {user.mention} has been removed from admins!", ephemeral=True)

@bot.hybrid_command(name='list_admins', description='List all admin users')
async def list_admins(ctx):
    """List all admin users"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return
    
    embed = discord.Embed(title="Admin Users", color=discord.Color.blue())
    
    # List user IDs in ADMIN_IDS
    admin_list = []
    for admin_id in ADMIN_IDS:
        try:
            user = await bot.fetch_user(admin_id)
            admin_list.append(f"{user.name} ({user.id})")
        except:
            admin_list.append(f"Unknown User ({admin_id})")
    
    # List users with admin role
    if ctx.guild:
        admin_role = ctx.guild.get_role(ADMIN_ROLE_ID)
        if admin_role:
            role_admins = [f"{member.name} ({member.id})" for member in admin_role.members]
            admin_list.extend(role_admins)
    
    if not admin_list:
        embed.description = "No admins found"
    else:
        embed.description = "\n".join(sorted(set(admin_list)))  # Remove duplicates
    
    await ctx.send(embed=embed, ephemeral=True)

class CreateVPSCredentialsModal(ui.Modal, title="VPS Credentials"):
    def __init__(self, memory, cpu, disk, owner, os_image, use_custom_image):
        super().__init__()
        self.memory = memory
        self.cpu = cpu
        self.disk = disk
        self.owner = owner
        self.os_image = os_image
        self.use_custom_image = use_custom_image
        self.username_input = ui.TextInput(label="Username", placeholder="Leave blank for default", required=False)
        self.password_input = ui.TextInput(label="Password", placeholder="Leave blank for default", required=False)
        self.add_item(self.username_input)
        self.add_item(self.password_input)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        username = self.username_input.value.strip() or None
        password = self.password_input.value.strip() or None
        await create_vps_flow(
            interaction,
            self.memory,
            self.cpu,
            self.disk,
            self.owner,
            self.os_image,
            self.use_custom_image,
            username,
            password,
        )


async def create_vps_flow(ctx, memory: int, cpu: int, disk: int, owner: discord.Member,
                         os_image: str, use_custom_image: bool,
                         username: Optional[str] = None, password: Optional[str] = None):
    """Create a VPS using the supplied or prompted credentials."""
    if not has_admin_role(ctx):
        await send_ephemeral_message(ctx, "❌ You must be an admin to use this command!")
        return

    if bot.db.is_user_banned(owner.id):
        await send_ephemeral_message(ctx, "❌ This user is banned from creating VPS!")
        return

    if not getattr(ctx, "guild", None):
        await send_ephemeral_message(ctx, "❌ This command can only be used in a server!")
        return

    if should_use_simulated_vps(bot.docker_client):
        await send_ephemeral_message(ctx, "⚠️ Docker is unavailable, so the VPS will be created in simulated mode. The instance will be registered for later provisioning.")
        vps_id = generate_vps_id()
        username, password = normalize_vps_credentials(username, password)
        token = generate_token()
        external_port = get_available_port(bot.db)
        vps_data = {
            "token": token,
            "vps_id": vps_id,
            "container_id": None,
            "memory": memory,
            "cpu": cpu,
            "disk": disk,
            "username": username,
            "password": password,
            "root_password": password,
            "created_by": str(owner.id),
            "created_at": str(datetime.datetime.now()),
            "tmate_session": None,
            "watermark": WATERMARK,
            "os_image": os_image,
            "restart_count": 0,
            "last_restart": None,
            "status": "simulated",
            "use_custom_image": use_custom_image,
            "external_ssh_port": external_port,
        }
        bot.db.add_vps(vps_data)
        bot.db.log_activity('create_vps', actor=str(owner.id), target=vps_id, detail=f"memory={memory} cpu={cpu} disk={disk} simulated")

        try:
            embed = discord.Embed(title="📝 VPS Registered in Simulated Mode", color=discord.Color.orange())
            embed.add_field(name="🆔 VPS ID", value=vps_id, inline=True)
            embed.add_field(name="💾 Memory", value=f"{memory}GB", inline=True)
            embed.add_field(name="⚡ CPU", value=f"{cpu} cores", inline=True)
            embed.add_field(name="💿 Disk", value=f"{disk}GB", inline=True)
            embed.add_field(name="👤 Username", value=username, inline=True)
            embed.add_field(name="🔑 Password", value=f"||{password}||", inline=False)
            embed.add_field(name="ℹ️ Note", value="Docker is unavailable right now, so this VPS was registered for later provisioning.", inline=False)
            await owner.send(embed=embed)
        except discord.Forbidden:
            logger.warning("Could not send simulated VPS details to owner")
        return

    try:
        ok, message = validate_vps_request(memory, cpu, disk)
        if not ok:
            if isinstance(ctx, discord.Interaction):
                await ctx.followup.send(f"❌ {message}", ephemeral=True)
            else:
                await ctx.send(f"❌ {message}", ephemeral=True)
            return

        containers = bot.docker_client.containers.list(all=True)
        if len(containers) >= bot.db.get_setting('max_containers', MAX_CONTAINERS):
            if isinstance(ctx, discord.Interaction):
                await ctx.followup.send(f"❌ Maximum container limit reached ({bot.db.get_setting('max_containers')}). Please delete some VPS instances first.", ephemeral=True)
            else:
                await ctx.send(f"❌ Maximum container limit reached ({bot.db.get_setting('max_containers')}). Please delete some VPS instances first.", ephemeral=True)
            return

        quota_limit = bot.db.get_quota_limit(owner.id, default=bot.db.get_setting('max_vps_per_user', MAX_VPS_PER_USER))
        if bot.db.get_user_vps_count(owner.id) >= quota_limit:
            if isinstance(ctx, discord.Interaction):
                await ctx.followup.send(f"❌ {owner.mention} already has the maximum number of VPS instances ({quota_limit})", ephemeral=True)
            else:
                await ctx.send(f"❌ {owner.mention} already has the maximum number of VPS instances ({quota_limit})", ephemeral=True)
            return

        if isinstance(ctx, discord.Interaction):
            await ctx.response.send_message("🚀 Creating HostForge VPS instance... This may take a few minutes.", ephemeral=True)
            status_msg = await ctx.original_response()
        else:
            status_msg = await ctx.send("🚀 Creating HostForge VPS instance... This may take a few minutes.")

        memory_bytes = memory * 1024 * 1024 * 1024
        vps_id = generate_vps_id()
        username, password = normalize_vps_credentials(username, password)
        token = generate_token()
        external_port = get_available_port(bot.db)

        if use_custom_image:
            await status_msg.edit(content="🔨 Building custom Docker image...")
            try:
                image_tag = await build_custom_image(vps_id, os_image)
            except Exception as e:
                await status_msg.edit(content=f"❌ Failed to build Docker image: {str(e)}")
                return

            await status_msg.edit(content="⚙️ Initializing container...")
            try:
                runtime_config = build_container_runtime_config(
                    memory_bytes=memory_bytes,
                    cpu_cores=cpu,
                    hostname=f"hostforge-{vps_id}",
                    network_name=DOCKER_NETWORK,
                    ports={'22/tcp': str(external_port)},
                    volumes={f'hostforge-{vps_id}': {'bind': '/data', 'mode': 'rw'}},
                )
                container = bot.docker_client.containers.run(image_tag, **runtime_config)
            except Exception as e:
                await status_msg.edit(content=f"❌ Failed to start container: {str(e)}")
                return
        else:
            await status_msg.edit(content="⚙️ Initializing container...")
            try:
                runtime_config = build_container_runtime_config(
                    memory_bytes=memory_bytes,
                    cpu_cores=cpu,
                    hostname=f"hostforge-{vps_id}",
                    network_name=DOCKER_NETWORK,
                    ports={'22/tcp': str(external_port)},
                    volumes={f'hostforge-{vps_id}': {'bind': '/data', 'mode': 'rw'}},
                )
                runtime_config.update({"command": "tail -f /dev/null", "tty": True})
                container = bot.docker_client.containers.run(os_image, **runtime_config)
            except docker.errors.ImageNotFound:
                await status_msg.edit(content=f"❌ OS image {os_image} not found. Using default {DEFAULT_OS_IMAGE}")
                runtime_config = build_container_runtime_config(
                    memory_bytes=memory_bytes,
                    cpu_cores=cpu,
                    hostname=f"hostforge-{vps_id}",
                    network_name=DOCKER_NETWORK,
                    ports={'22/tcp': str(external_port)},
                    volumes={f'hostforge-{vps_id}': {'bind': '/data', 'mode': 'rw'}},
                )
                runtime_config.update({"command": "tail -f /dev/null", "tty": True})
                container = bot.docker_client.containers.run(DEFAULT_OS_IMAGE, **runtime_config)
                os_image = DEFAULT_OS_IMAGE

        await status_msg.edit(content="🔧 Container created. Setting up HostForge environment...")
        await asyncio.sleep(2)

        setup_success, _ = await setup_container(container.id, status_msg, memory, vps_id=vps_id, use_custom_image=use_custom_image)
        if not setup_success:
            raise Exception("Failed to setup container")

        await status_msg.edit(content="🔐 Starting SSH session...")
        exec_cmd = await asyncio.create_subprocess_exec(
            "docker", "exec", container.id, "tmate", "-F",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        ssh_session_line = await capture_ssh_session_line(exec_cmd)
        if not ssh_session_line:
            raise Exception("Failed to get tmate session")

        vps_data = {
            "token": token,
            "vps_id": vps_id,
            "container_id": container.id,
            "memory": memory,
            "cpu": cpu,
            "disk": disk,
            "username": username,
            "password": password,
            "root_password": password,
            "created_by": str(owner.id),
            "created_at": str(datetime.datetime.now()),
            "tmate_session": ssh_session_line,
            "watermark": WATERMARK,
            "os_image": os_image,
            "restart_count": 0,
            "last_restart": None,
            "status": "running",
            "use_custom_image": use_custom_image,
            "external_ssh_port": external_port,
        }

        bot.db.add_vps(vps_data)
        bot.db.log_activity('create_vps', actor=str(owner.id), target=vps_id, detail=f"memory={memory} cpu={cpu} disk={disk}")

        try:
            embed = discord.Embed(title="🎉 HostForge VPS Creation Successful", color=discord.Color.green())
            embed.add_field(name="🆔 VPS ID", value=vps_id, inline=True)
            embed.add_field(name="💾 Memory", value=f"{memory}GB", inline=True)
            embed.add_field(name="⚡ CPU", value=f"{cpu} cores", inline=True)
            embed.add_field(name="💿 Disk", value=f"{disk}GB", inline=True)
            embed.add_field(name="👤 Username", value=username, inline=True)
            embed.add_field(name="🔑 Password", value=f"||{password}||", inline=False)
            embed.add_field(name="🔒 Tmate Session", value=f"```{ssh_session_line}```", inline=False)
            embed.add_field(name="🔌 Direct SSH", value=f"```ssh root@{bot.public_ip} -p {external_port}```", inline=False)
            embed.add_field(name="ℹ️ Note", value="This is a HostForge VPS instance. You can install and configure additional packages as needed.", inline=False)
            await owner.send(embed=embed)
            await status_msg.edit(content=f"✅ HostForge VPS creation successful! VPS has been created for {owner.mention}. Check your DMs for connection details.")
        except discord.Forbidden:
            await status_msg.edit(content=f"❌ I couldn't send a DM to {owner.mention}. Please ask them to enable DMs from server members.")
    except Exception as e:
        error_msg = f"❌ An error occurred while creating the VPS: {str(e)}"
        logger.error(error_msg)
        if isinstance(ctx, discord.Interaction):
            if not ctx.response.is_done():
                await ctx.response.send_message(error_msg, ephemeral=True)
            else:
                await ctx.followup.send(error_msg, ephemeral=True)
        else:
            await ctx.send(error_msg)
        if 'container' in locals():
            try:
                container.stop()
                container.remove()
            except Exception as e:
                logger.error(f"Error cleaning up container: {e}")


@bot.hybrid_command(name='create_vps', description='Create a new VPS (Admin only)')
@app_commands.describe(
    memory="Memory in GB",
    cpu="CPU cores",
    disk="Disk space in GB",
    owner="User who will own the VPS",
    os_image="OS image to use",
    use_custom_image="Use custom HostForge image (recommended)",
    username="Optional VPS login username",
    password="Optional VPS login password"
)
async def create_vps_command(ctx, memory: int, cpu: int, disk: int, owner: discord.Member, 
                           os_image: str = DEFAULT_OS_IMAGE, use_custom_image: bool = True,
                           username: Optional[str] = None, password: Optional[str] = None):
    """Create a new VPS with specified parameters (Admin only)"""
    if is_interaction_context(ctx):
        if not has_vps_credentials(username, password):
            modal = CreateVPSCredentialsModal(memory, cpu, disk, owner, os_image, use_custom_image)
            await ctx.response.send_modal(modal)
            return
        await create_vps_flow(ctx, memory, cpu, disk, owner, os_image, use_custom_image, username, password)
        return

    if not has_vps_credentials(username, password):
        username, password = normalize_vps_credentials(None, None)

    await create_vps_flow(ctx, memory, cpu, disk, owner, os_image, use_custom_image, username, password)

@bot.hybrid_command(name='list', description='List all your VPS instances')
async def list_vps(ctx):
    """List all VPS instances owned by the user"""
    try:
        user_vps = bot.db.get_user_vps(ctx.author.id)
        
        if not user_vps:
            await ctx.send("You don't have any VPS instances.", ephemeral=True)
            return

        embed = discord.Embed(title="Your HostForge VPS Instances", color=discord.Color.blue())
        
        for vps in user_vps:
            try:
                # Handle missing container ID gracefully
                container = bot.docker_client.containers.get(vps["container_id"]) if vps["container_id"] else None
                status = vps['status'].capitalize() if vps.get('status') else "Unknown"
            except Exception as e:
                status = "Not Found"
                logger.error(f"Error fetching container {vps['container_id']}: {e}")

            # Adding fields safely to prevent missing keys causing errors
            embed.add_field(
                name=f"VPS {vps['vps_id']}",
                value=f"""
Status: {status}
Memory: {vps.get('memory', 'Unknown')}GB
CPU: {vps.get('cpu', 'Unknown')} cores
Disk Allocated: {vps.get('disk', 'Unknown')}GB
Username: {vps.get('username', 'Unknown')}
OS: {vps.get('os_image', DEFAULT_OS_IMAGE)}
Created: {vps.get('created_at', 'Unknown')}
Restarts: {vps.get('restart_count', 0)}
SSH Port: {vps.get('external_ssh_port', 'Not set')}
""",
                inline=False
            )
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in list_vps: {e}")
        await ctx.send(f"❌ Error listing VPS instances: {str(e)}")

@bot.hybrid_command(name='vps_list', description='List all VPS instances (Admin only)')
async def admin_list_vps(ctx):
    """List all VPS instances (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        all_vps = bot.db.get_all_vps()
        if not all_vps:
            await ctx.send("No VPS instances found.", ephemeral=True)
            return

        embed = discord.Embed(title="All HostForge VPS Instances", color=discord.Color.blue())
        valid_vps_count = 0
        
        for token, vps in all_vps.items():
            try:
                # Fetch username of the owner with error handling
                user = await bot.fetch_user(int(vps.get("created_by", "0")))
                username = user.name if user else "Unknown User"
            except Exception as e:
                username = "Unknown User"
                logger.error(f"Error fetching user {vps.get('created_by')}: {e}")

            try:
                # Handle missing container ID gracefully
                container = bot.docker_client.containers.get(vps.get("container_id", "")) if vps.get("container_id") else None
                container_status = container.status if container else "Not Found"
            except Exception as e:
                container_status = "Not Found"
                logger.error(f"Error fetching container {vps.get('container_id')}: {e}")

            # Get status and other info with error fallback
            status = vps.get('status', "Unknown").capitalize()

            vps_info = f"""
Owner: {username}
Status: {status} (Container: {container_status})
Memory: {vps.get('memory', 'Unknown')}GB
CPU: {vps.get('cpu', 'Unknown')} cores
Disk: {vps.get('disk', 'Unknown')}GB
Username: {vps.get('username', 'Unknown')}
OS: {vps.get('os_image', DEFAULT_OS_IMAGE)}
Created: {vps.get('created_at', 'Unknown')}
Restarts: {vps.get('restart_count', 0)}
SSH Port: {vps.get('external_ssh_port', 'Not set')}
"""

            embed.add_field(
                name=f"VPS {vps.get('vps_id', 'Unknown')}",
                value=vps_info,
                inline=False
            )
            valid_vps_count += 1

        if valid_vps_count == 0:
            await ctx.send("No valid VPS instances found.", ephemeral=True)
            return

        embed.set_footer(text=f"Total VPS instances: {valid_vps_count}")
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in admin_list_vps: {e}")
        await ctx.send(f"❌ Error listing VPS instances: {str(e)}")

@bot.hybrid_command(name='delete_vps', description='Delete a VPS instance (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to delete"
)
async def delete_vps(ctx, vps_id: str):
    """Delete a VPS instance (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return
        
        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            container.stop()
            container.remove()
            logger.info(f"Deleted container {vps['container_id']} for VPS {vps_id}")
        except Exception as e:
            logger.error(f"Error removing container: {e}")
        
        bot.db.remove_vps(token)
        
        await ctx.send(f"✅ HostForge VPS {vps_id} has been deleted successfully!")
    except Exception as e:
        logger.error(f"Error in delete_vps: {e}")
        await ctx.send(f"❌ Error deleting VPS: {str(e)}")

@bot.hybrid_command(name='connect_vps', description='Connect to a VPS using the provided token')
@app_commands.describe(
    token="Access token for the VPS"
)
async def connect_vps(ctx, token: str):
    """Connect to a VPS using the provided token"""
    vps = bot.db.get_vps_by_token(token)
    if not vps:
        await ctx.send("❌ Invalid token!", ephemeral=True)
        return
        
    if str(ctx.author.id) != vps["created_by"] and not has_admin_role(ctx):
        await ctx.send("❌ You don't have permission to access this VPS!", ephemeral=True)
        return

    try:
        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                container.start()
                await asyncio.sleep(5)
        except:
            await ctx.send("❌ VPS instance not found or is no longer available.", ephemeral=True)
            return

        exec_cmd = await asyncio.create_subprocess_exec(
            "docker", "exec", vps["container_id"], "tmate", "-F",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        ssh_session_line = await capture_ssh_session_line(exec_cmd)
        if not ssh_session_line:
            raise Exception("Failed to get tmate session")

        bot.db.update_vps(token, {"tmate_session": ssh_session_line})
        
        embed = discord.Embed(title="HostForge VPS Connection Details", color=discord.Color.blue())
        embed.add_field(name="Username", value=vps["username"], inline=True)
        embed.add_field(name="SSH Password", value=f"||{vps.get('password', 'Not set')}||", inline=True)
        embed.add_field(name="Tmate Session", value=f"```{ssh_session_line}```", inline=False)
        embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
        embed.add_field(name="Connection Instructions", value="""
1. Copy the Tmate session command
2. Open your terminal
3. Paste and run the command
4. You will be connected to your HostForge VPS

Or use direct SSH with the provided command.
""", inline=False)
        
        await ctx.author.send(embed=embed)
        await ctx.send("✅ Connection details sent to your DMs! Use the Tmate command to connect to your HostForge VPS.", ephemeral=True)
        
    except discord.Forbidden:
        await ctx.send("❌ I couldn't send you a DM. Please enable DMs from server members.", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in connect_vps: {e}")
        await ctx.send(f"❌ An error occurred while connecting to the VPS: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='vps_stats', description='Show resource usage for a VPS')
@app_commands.describe(
    vps_id="ID of the VPS to check"
)
async def vps_stats(ctx, vps_id: str):
    """Show resource usage for a VPS"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps["created_by"] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("❌ VPS is not running!", ephemeral=True)
                return

            # Get memory stats
            mem_process = await asyncio.create_subprocess_exec(
                "docker", "exec", vps["container_id"], "free", "-m",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await mem_process.communicate()
            
            if mem_process.returncode != 0:
                raise Exception(f"Failed to get memory info: {stderr.decode()}")

            # Get CPU stats
            cpu_process = await asyncio.create_subprocess_exec(
                "docker", "exec", vps["container_id"], "top", "-bn1",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            cpu_stdout, cpu_stderr = await cpu_process.communicate()

            # Get disk stats
            disk_process = await asyncio.create_subprocess_exec(
                "docker", "exec", vps["container_id"], "df", "-h",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            disk_stdout, disk_stderr = await disk_process.communicate()

            embed = discord.Embed(title=f"Resource Usage for VPS {vps_id}", color=discord.Color.blue())
            embed.add_field(name="Memory Info", value=f"```{stdout.decode()}```", inline=False)
            
            if disk_process.returncode == 0:
                embed.add_field(name="Disk Info", value=f"```{disk_stdout.decode()}```", inline=False)
            
            embed.add_field(name="Configured Limits", value=f"""
Memory: {vps['memory']}GB
CPU: {vps['cpu']} cores
Disk Allocated: {vps['disk']}GB
SSH Port: {vps['external_ssh_port']}
""", inline=True)
            
            await ctx.send(embed=embed)
        except Exception as e:
            await ctx.send(f"❌ Error checking VPS stats: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in vps_stats: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='change_ssh_password', description='Change the SSH password for a VPS')
@app_commands.describe(
    vps_id="ID of the VPS to update"
)
async def change_ssh_password(ctx, vps_id: str):
    """Change the SSH password for a VPS"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or vps["created_by"] != str(ctx.author.id):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("❌ VPS is not running!", ephemeral=True)
                return

            new_password = generate_ssh_password()
            
            process = await asyncio.create_subprocess_exec(
                "docker", "exec", vps["container_id"], "bash", "-c", f"echo 'root:{new_password}' | chpasswd",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            
            if process.returncode != 0:
                raise Exception(f"Failed to change password: {stderr.decode()}")

            bot.db.update_vps(token, {'password': new_password})
            
            embed = discord.Embed(title=f"SSH Password Updated for VPS {vps_id}", color=discord.Color.green())
            embed.add_field(name="Username", value="root", inline=True)
            embed.add_field(name="New Password", value=f"||{new_password}||", inline=False)
            embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
            
            await ctx.author.send(embed=embed)
            await ctx.send("✅ SSH password updated successfully! Check your DMs for the new password.", ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error changing SSH password: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in change_ssh_password: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='admin_stats', description='Show system statistics (Admin only)')
async def admin_stats(ctx):
    """Show system statistics (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        # Get Docker stats
        containers = bot.docker_client.containers.list(all=True) if bot.docker_client else []
        
        # Get system stats
        stats = bot.system_stats
        
        embed = discord.Embed(title="HostForge System Statistics", color=discord.Color.blue())
        embed.add_field(name="VPS Instances", value=f"Total: {len(bot.db.get_all_vps())}\nRunning: {len([c for c in containers if c.status == 'running'])}", inline=True)
        embed.add_field(name="Docker Containers", value=f"Total: {len(containers)}\nRunning: {len([c for c in containers if c.status == 'running'])}", inline=True)
        embed.add_field(name="CPU Usage", value=f"{stats['cpu_usage']}%", inline=True)
        embed.add_field(name="Memory Usage", value=f"{stats['memory_usage']}% ({stats['memory_used']:.2f}GB / {stats['memory_total']:.2f}GB)", inline=True)
        embed.add_field(name="Disk Usage", value=f"{stats['disk_usage']}% ({stats['disk_used']:.2f}GB / {stats['disk_total']:.2f}GB)", inline=True)
        embed.add_field(name="Network", value=f"Sent: {stats['network_sent']:.2f}MB\nRecv: {stats['network_recv']:.2f}MB", inline=True)
        embed.add_field(name="Container Limit", value=f"{len(containers)}/{bot.db.get_setting('max_containers')}", inline=True)
        embed.add_field(name="Last Updated", value=f"<t:{int(stats['last_updated'])}:R>", inline=True)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in admin_stats: {e}")
        await ctx.send(f"❌ Error getting system stats: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='system_info', description='Show detailed system information (Admin only)')
async def system_info(ctx):
    """Show detailed system information (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        # System information
        uname = platform.uname()
        boot_time = datetime.datetime.fromtimestamp(psutil.boot_time())
        
        # CPU information
        cpu_info = f"""
System: {uname.system}
Node Name: {uname.node}
Release: {uname.release}
Version: {uname.version}
Machine: {uname.machine}
Processor: {uname.processor}
Physical cores: {psutil.cpu_count(logical=False)}
Total cores: {psutil.cpu_count(logical=True)}
CPU Usage: {psutil.cpu_percent()}%
"""
        
        # Memory Information
        svmem = psutil.virtual_memory()
        mem_info = f"""
Total: {svmem.total / (1024**3):.2f}GB
Available: {svmem.available / (1024**3):.2f}GB
Used: {svmem.used / (1024**3):.2f}GB
Percentage: {svmem.percent}%
"""
        
        # Disk Information
        partitions = psutil.disk_partitions()
        disk_info = ""
        for partition in partitions:
            try:
                partition_usage = psutil.disk_usage(partition.mountpoint)
                disk_info += f"""
Device: {partition.device}
  Mountpoint: {partition.mountpoint}
  File system type: {partition.fstype}
  Total Size: {partition_usage.total / (1024**3):.2f}GB
  Used: {partition_usage.used / (1024**3):.2f}GB
  Free: {partition_usage.free / (1024**3):.2f}GB
  Percentage: {partition_usage.percent}%
"""
            except PermissionError:
                continue
        
        # Network information
        net_io = psutil.net_io_counters()
        net_info = f"""
Bytes Sent: {net_io.bytes_sent / (1024**2):.2f}MB
Bytes Received: {net_io.bytes_recv / (1024**2):.2f}MB
"""
        
        embed = discord.Embed(title="Detailed System Information", color=discord.Color.blue())
        embed.add_field(name="System", value=f"Boot Time: {boot_time}", inline=False)
        embed.add_field(name="CPU Info", value=f"```{cpu_info}```", inline=False)
        embed.add_field(name="Memory Info", value=f"```{mem_info}```", inline=False)
        embed.add_field(name="Disk Info", value=f"```{disk_info}```", inline=False)
        embed.add_field(name="Network Info", value=f"```{net_info}```", inline=False)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in system_info: {e}")
        await ctx.send(f"❌ Error getting system info: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='container_limit', description='Set maximum container limit (Owner only)')
@app_commands.describe(
    max_limit="New maximum container limit"
)
async def set_container_limit(ctx, max_limit: int):
    """Set maximum container limit (Owner only)"""
    if ctx.author.id != 1210291131301101618:  # Only the owner can set limit
        await ctx.send("❌ Only the owner can set container limit!", ephemeral=True)
        return
    
    if max_limit < 1 or max_limit > 1000:
        await ctx.send("❌ Container limit must be between 1 and 1000", ephemeral=True)
        return
    
    bot.db.set_setting('max_containers', max_limit)
    await ctx.send(f"✅ Maximum container limit set to {max_limit}", ephemeral=True)

@bot.hybrid_command(name='cleanup_vps', description='Cleanup inactive VPS instances (Admin only)')
async def cleanup_vps(ctx):
    """Cleanup inactive VPS instances (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        cleanup_count = 0
        
        for token, vps in list(bot.db.get_all_vps().items()):
            try:
                container = bot.docker_client.containers.get(vps['container_id'])
                if container.status != 'running':
                    container.stop()
                    container.remove()
                    bot.db.remove_vps(token)
                    cleanup_count += 1
            except docker.errors.NotFound:
                bot.db.remove_vps(token)
                cleanup_count += 1
            except Exception as e:
                logger.error(f"Error cleaning up VPS {vps['vps_id']}: {e}")
                continue
        
        if cleanup_count > 0:
            await ctx.send(f"✅ Cleaned up {cleanup_count} inactive VPS instances!")
        else:
            await ctx.send("ℹ️ No inactive VPS instances found to clean up.")
    except Exception as e:
        logger.error(f"Error in cleanup_vps: {e}")
        await ctx.send(f"❌ Error during cleanup: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='vps_shell', description='Get shell access to your VPS')
@app_commands.describe(
    vps_id="ID of the VPS to access"
)
async def vps_shell(ctx, vps_id: str):
    """Get shell access to your VPS"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps["created_by"] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("❌ VPS is not running!", ephemeral=True)
                return

            await ctx.send(f"✅ Shell access to VPS {vps_id}:\n"
                          f"```docker exec -it {vps['container_id']} bash```\n"
                          f"Username: root\n"
                          f"Password: ||{vps.get('password', 'Not set')}||", ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error accessing VPS shell: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in vps_shell: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='vps_console', description='Get direct console access to your VPS')
@app_commands.describe(
    vps_id="ID of the VPS to access"
)
async def vps_console(ctx, vps_id: str):
    """Get direct console access to your VPS"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps["created_by"] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("❌ VPS is not running!", ephemeral=True)
                return

            await ctx.send(f"✅ Console access to VPS {vps_id}:\n"
                          f"```docker attach {vps['container_id']}```\n"
                          f"Note: To detach from the console without stopping the container, use Ctrl+P followed by Ctrl+Q", 
                          ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error accessing VPS console: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in vps_console: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='run_command', description='Run a command in your VPS')
@app_commands.describe(
    vps_id="ID of the VPS",
    command="Command to run"
)
async def run_command(ctx, vps_id: str, command: str):
    """Run a command in your VPS"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps["created_by"] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("❌ VPS is not running!", ephemeral=True)
                return

            success, output = await run_docker_command(vps["container_id"], ["bash", "-c", command])
            if success:
                await ctx.send(f"✅ Command executed successfully:\n```{output}```", ephemeral=True)
            else:
                await ctx.send(f"❌ Command failed:\n```{output}```", ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error running command: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in run_command: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='vps_usage', description='Show your VPS usage statistics')
async def vps_usage(ctx):
    """Show your VPS usage statistics"""
    try:
        user_vps = bot.db.get_user_vps(ctx.author.id)
        
        total_memory = sum(vps['memory'] for vps in user_vps)
        total_cpu = sum(vps['cpu'] for vps in user_vps)
        total_disk = sum(vps['disk'] for vps in user_vps)
        total_restarts = sum(vps.get('restart_count', 0) for vps in user_vps)
        
        embed = discord.Embed(title="Your HostForge VPS Usage", color=discord.Color.blue())
        embed.add_field(name="Total VPS Instances", value=len(user_vps), inline=True)
        embed.add_field(name="Total Memory Allocated", value=f"{total_memory}GB", inline=True)
        embed.add_field(name="Total CPU Cores Allocated", value=total_cpu, inline=True)
        embed.add_field(name="Total Disk Allocated", value=f"{total_disk}GB", inline=True)
        embed.add_field(name="Total Restarts", value=total_restarts, inline=True)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in vps_usage: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='global_stats', description='Show global usage statistics (Admin only)')
async def global_stats(ctx):
    """Show global usage statistics (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        all_vps = bot.db.get_all_vps()
        total_memory = sum(vps['memory'] for vps in all_vps.values())
        total_cpu = sum(vps['cpu'] for vps in all_vps.values())
        total_disk = sum(vps['disk'] for vps in all_vps.values())
        total_restarts = sum(vps.get('restart_count', 0) for vps in all_vps.values())
        
        embed = discord.Embed(title="HostForge Global Usage Statistics", color=discord.Color.blue())
        embed.add_field(name="Total VPS Created", value=bot.db.get_stat('total_vps_created'), inline=True)
        embed.add_field(name="Total Restarts", value=bot.db.get_stat('total_restarts'), inline=True)
        embed.add_field(name="Current VPS Instances", value=len(all_vps), inline=True)
        embed.add_field(name="Total Memory Allocated", value=f"{total_memory}GB", inline=True)
        embed.add_field(name="Total CPU Cores Allocated", value=total_cpu, inline=True)
        embed.add_field(name="Total Disk Allocated", value=f"{total_disk}GB", inline=True)
        embed.add_field(name="Total Restarts", value=total_restarts, inline=True)
        
        await ctx.send(embed=embed)
    except Exception as e:
        logger.error(f"Error in global_stats: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='migrate_vps', description='Migrate a VPS to another host (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to migrate"
)
async def migrate_vps(ctx, vps_id: str):
    """Migrate a VPS to another host (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        status_msg = await ctx.send(f"🔄 Preparing to migrate VPS {vps_id}...")
        
        # Create a snapshot
        backup_id = generate_vps_id()[:8]
        backup_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        backup_dir = f"migrations/{vps_id}"
        os.makedirs(backup_dir, exist_ok=True)
        backup_file = f"{backup_dir}/{backup_id}.tar"
        
        await status_msg.edit(content=f"🔄 Creating snapshot {backup_id} for migration...")
        
        process = await asyncio.create_subprocess_exec(
            "docker", "export", "-o", backup_file, vps["container_id"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Snapshot failed: {stderr.decode()}")
        
        await status_msg.edit(content=f"✅ Snapshot {backup_id} created successfully. Please download this file and import it on the new host: {backup_file}")
        
    except Exception as e:
        logger.error(f"Error in migrate_vps: {e}")
        await ctx.send(f"❌ Error during migration: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='emergency_stop', description='Force stop a problematic VPS (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to stop"
)
async def emergency_stop(ctx, vps_id: str):
    """Force stop a problematic VPS (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            if container.status != "running":
                await ctx.send("VPS is already stopped!", ephemeral=True)
                return
            
            await ctx.send("⚠️ Attempting to force stop the VPS... This may take a moment.", ephemeral=True)
            
            # Try normal stop first
            try:
                container.stop(timeout=10)
                bot.db.update_vps(token, {'status': 'stopped'})
                await ctx.send("✅ VPS stopped successfully!", ephemeral=True)
                return
            except:
                pass
            
            # If normal stop failed, try killing the container
            try:
                subprocess.run(["docker", "kill", vps["container_id"]], check=True)
                bot.db.update_vps(token, {'status': 'stopped'})
                await ctx.send("✅ VPS killed forcefully!", ephemeral=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to kill container: {e}")
            
        except Exception as e:
            await ctx.send(f"❌ Error stopping VPS: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in emergency_stop: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='emergency_remove', description='Force remove a problematic VPS (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to remove"
)
async def emergency_remove(ctx, vps_id: str):
    """Force remove a problematic VPS (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        try:
            # First try to stop the container
            try:
                container = bot.docker_client.containers.get(vps["container_id"])
                container.stop()
            except:
                pass
            
            # Then remove it forcefully
            try:
                subprocess.run(["docker", "rm", "-f", vps["container_id"]], check=True)
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to remove container: {e}")
            
            # Remove from data
            bot.db.remove_vps(token)
            
            await ctx.send("✅ VPS removed forcefully!", ephemeral=True)
        except Exception as e:
            await ctx.send(f"❌ Error removing VPS: {str(e)}", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in emergency_remove: {e}")
        await ctx.send(f"❌ Error: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='suspend_vps', description='Suspend a VPS (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to suspend"
)
async def suspend_vps(ctx, vps_id: str):
    """Suspend a VPS (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        if vps['status'] == 'suspended':
            await ctx.send("❌ VPS is already suspended!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            container.stop()
        except Exception as e:
            logger.error(f"Error stopping container for suspend: {e}")

        bot.db.update_vps(token, {'status': 'suspended'})
        await ctx.send(f"✅ VPS {vps_id} has been suspended!")

        # Notify owner
        try:
            owner = await bot.fetch_user(int(vps['created_by']))
            await owner.send(f"⚠️ Your VPS {vps_id} has been suspended by an admin. Contact support for details.")
        except:
            pass

    except Exception as e:
        logger.error(f"Error in suspend_vps: {e}")
        await ctx.send(f"❌ Error suspending VPS: {str(e)}")

@bot.hybrid_command(name='unsuspend_vps', description='Unsuspend a VPS (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to unsuspend"
)
async def unsuspend_vps(ctx, vps_id: str):
    """Unsuspend a VPS (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        if vps['status'] != 'suspended':
            await ctx.send("❌ VPS is not suspended!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            container.start()
        except Exception as e:
            logger.error(f"Error starting container for unsuspend: {e}")
            await ctx.send(f"❌ Error starting container: {str(e)}")
            return

        bot.db.update_vps(token, {'status': 'running'})
        await ctx.send(f"✅ VPS {vps_id} has been unsuspended!")

        # Notify owner
        try:
            owner = await bot.fetch_user(int(vps['created_by']))
            await owner.send(f"✅ Your VPS {vps_id} has been unsuspended by an admin.")
        except:
            pass

    except Exception as e:
        logger.error(f"Error in unsuspend_vps: {e}")
        await ctx.send(f"❌ Error unsuspending VPS: {str(e)}")

@bot.hybrid_command(name='edit_vps', description='Edit VPS specifications (Admin only)')
@app_commands.describe(
    vps_id="ID of the VPS to edit",
    memory="New memory in GB (optional)",
    cpu="New CPU cores (optional)",
    disk="New disk space in GB (optional)"
)
async def edit_vps(ctx, vps_id: str, memory: Optional[int] = None, cpu: Optional[int] = None, disk: Optional[int] = None):
    """Edit VPS specifications (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    if memory is None and cpu is None and disk is None:
        await ctx.send("❌ At least one specification to edit must be provided!", ephemeral=True)
        return

    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps:
            await ctx.send("❌ VPS not found!", ephemeral=True)
            return

        updates = {}
        if memory is not None:
            if memory < 1 or memory > 512:
                await ctx.send("❌ Memory must be between 1GB and 512GB", ephemeral=True)
                return
            updates['memory'] = memory
        if cpu is not None:
            if cpu < 1 or cpu > 32:
                await ctx.send("❌ CPU cores must be between 1 and 32", ephemeral=True)
                return
            updates['cpu'] = cpu
        if disk is not None:
            if disk < 10 or disk > 1000:
                await ctx.send("❌ Disk space must be between 10GB and 1000GB", ephemeral=True)
                return
            updates['disk'] = disk

        # Restart container with new limits
        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            container.stop()
            container.remove()

            memory_bytes = (memory or vps['memory']) * 1024 * 1024 * 1024
            cpu_quota = int((cpu or vps['cpu']) * 100000)

            new_container = bot.docker_client.containers.run(
                vps['os_image'],
                detach=True,
                privileged=True,
                hostname=f"hostforge-{vps_id}",
                mem_limit=memory_bytes,
                cpu_period=100000,
                cpu_quota=cpu_quota,
                cap_add=["ALL"],
                command="tail -f /dev/null",
                tty=True,
                network=DOCKER_NETWORK,
                ports={'22/tcp': str(vps['external_ssh_port'])},
                volumes={
                    f'hostforge-{vps_id}': {'bind': '/data', 'mode': 'rw'}
                },
                restart_policy={"Name": "always"}
            )

            updates['container_id'] = new_container.id
            await asyncio.sleep(5)
            setup_success, _ = await setup_container(
                new_container.id, 
                ctx, 
                memory or vps['memory'], 
                vps_id=vps_id,
                use_custom_image=vps['use_custom_image']
            )
            if not setup_success:
                raise Exception("Failed to setup new container")
        except Exception as e:
            await ctx.send(f"❌ Error updating container: {str(e)}")
            return

        bot.db.update_vps(token, updates)
        await ctx.send(f"✅ VPS {vps_id} specifications updated successfully!")

    except Exception as e:
        logger.error(f"Error in edit_vps: {e}")
        await ctx.send(f"❌ Error editing VPS: {str(e)}")

@bot.hybrid_command(name='ban_user', description='Ban a user from creating VPS (Admin only)')
@app_commands.describe(
    user="User to ban"
)
async def ban_user(ctx, user: discord.User):
    """Ban a user from creating VPS (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    bot.db.ban_user(user.id)
    await ctx.send(f"✅ {user.mention} has been banned from creating VPS!")

@bot.hybrid_command(name='unban_user', description='Unban a user (Admin only)')
@app_commands.describe(
    user="User to unban"
)
async def unban_user(ctx, user: discord.User):
    """Unban a user (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    bot.db.unban_user(user.id)
    await ctx.send(f"✅ {user.mention} has been unbanned!")

@bot.hybrid_command(name='list_banned', description='List banned users (Admin only)')
async def list_banned(ctx):
    """List banned users (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    banned = bot.db.get_banned_users()
    if not banned:
        await ctx.send("No banned users.", ephemeral=True)
        return

    embed = discord.Embed(title="Banned Users", color=discord.Color.red())
    banned_list = []
    for user_id in banned:
        try:
            user = await bot.fetch_user(int(user_id))
            banned_list.append(f"{user.name} ({user_id})")
        except:
            banned_list.append(f"Unknown ({user_id})")
    embed.description = "\n".join(banned_list)
    await ctx.send(embed=embed, ephemeral=True)

@bot.hybrid_command(name='backup_data', description='Backup all bot data (Admin only)')
async def backup_data(ctx):
    """Backup all bot data (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        if bot.db.backup_data():
            await ctx.send("✅ Data backup completed successfully!", ephemeral=True)
        else:
            await ctx.send("❌ Failed to backup data!", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in backup_data: {e}")
        await ctx.send(f"❌ Error backing up data: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='restore_data', description='Restore from backup (Admin only)')
async def restore_data(ctx):
    """Restore from backup (Admin only)"""
    if not has_admin_role(ctx):
        await ctx.send("❌ You must be an admin to use this command!", ephemeral=True)
        return

    try:
        if bot.db.restore_data():
            await ctx.send("✅ Data restore completed successfully!", ephemeral=True)
        else:
            await ctx.send("❌ Failed to restore data!", ephemeral=True)
    except Exception as e:
        logger.error(f"Error in restore_data: {e}")
        await ctx.send(f"❌ Error restoring data: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='reinstall_bot', description='Reinstall the bot (Owner only)')
async def reinstall_bot(ctx):
    """Reinstall the bot (Owner only)"""
    if ctx.author.id != 1210291131301101618:  # Only the owner can reinstall
        await ctx.send("❌ Only the owner can reinstall the bot!", ephemeral=True)
        return

    try:
        await ctx.send("🔄 Reinstalling HostForge bot... This may take a few minutes.")
        
        # Create Dockerfile for bot reinstallation
        dockerfile_content = f"""
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    docker.io \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Start the bot
CMD ["python", "bot.py"]
"""
        
        with open("Dockerfile.bot", "w") as f:
            f.write(dockerfile_content)
        
        # Build and run the bot in a container
        process = await asyncio.create_subprocess_exec(
            "docker", "build", "-t", "hostforge-bot", "-f", "Dockerfile.bot", ".",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Failed to build bot image: {stderr.decode()}")
        
        await ctx.send("✅ Bot reinstalled successfully! Restarting...")
        
        # Restart the bot
        os._exit(0)
        
    except Exception as e:
        logger.error(f"Error in reinstall_bot: {e}")
        await ctx.send(f"❌ Error reinstalling bot: {str(e)}", ephemeral=True)

class VPSManagementView(ui.View):
    def __init__(self, vps_id, container_id):
        super().__init__(timeout=300)
        self.vps_id = vps_id
        self.container_id = container_id
        self.original_message = None

    async def handle_missing_container(self, interaction: discord.Interaction):
        token, _ = bot.db.get_vps_by_id(self.vps_id)
        if token:
            bot.db.remove_vps(token)
        
        embed = discord.Embed(title=f"HostForge VPS Management - {self.vps_id}", color=discord.Color.red())
        embed.add_field(name="Status", value="🔴 Container Not Found", inline=True)
        embed.add_field(name="Note", value="This VPS instance is no longer available. Please create a new one.", inline=False)
        
        for item in self.children:
            item.disabled = True
        
        await interaction.message.edit(embed=embed, view=self)
        await interaction.response.send_message("❌ This VPS instance is no longer available. Please create a new one.", ephemeral=True)

    @discord.ui.button(label="Start VPS", style=discord.ButtonStyle.green)
    async def start_vps(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer(ephemeral=True)
            
            try:
                container = bot.docker_client.containers.get(self.container_id)
            except docker.errors.NotFound:
                await self.handle_missing_container(interaction)
                return
            
            token, vps = bot.db.get_vps_by_id(self.vps_id)
            if vps['status'] == 'suspended':
                await interaction.followup.send("❌ This VPS is suspended. Contact admin to unsuspend.", ephemeral=True)
                return

            if container.status == "running":
                await interaction.followup.send("VPS is already running!", ephemeral=True)
                return
            
            container.start()
            await asyncio.sleep(5)
            
            if token:
                bot.db.update_vps(token, {'status': 'running'})
            
            embed = discord.Embed(title=f"HostForge VPS Management - {self.vps_id}", color=discord.Color.green())
            embed.add_field(name="Status", value="🟢 Running", inline=True)
            
            if vps:
                embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
                embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
                embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
                embed.add_field(name="Username", value=vps['username'], inline=True)
                embed.add_field(name="Created", value=vps['created_at'], inline=True)
            
            await interaction.message.edit(embed=embed)
            await interaction.followup.send("✅ HostForge VPS started successfully!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error starting VPS: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Stop VPS", style=discord.ButtonStyle.red)
    async def stop_vps(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer(ephemeral=True)
            
            try:
                container = bot.docker_client.containers.get(self.container_id)
            except docker.errors.NotFound:
                await self.handle_missing_container(interaction)
                return
            
            if container.status != "running":
                await interaction.followup.send("VPS is already stopped!", ephemeral=True)
                return
            
            container.stop()
            
            token, vps = bot.db.get_vps_by_id(self.vps_id)
            if token:
                bot.db.update_vps(token, {'status': 'stopped'})
            
            embed = discord.Embed(title=f"HostForge VPS Management - {self.vps_id}", color=discord.Color.orange())
            embed.add_field(name="Status", value="🔴 Stopped", inline=True)
            
            if vps:
                embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
                embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
                embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
                embed.add_field(name="Username", value=vps['username'], inline=True)
                embed.add_field(name="Created", value=vps['created_at'], inline=True)
            
            await interaction.message.edit(embed=embed)
            await interaction.followup.send("✅ HostForge VPS stopped successfully!", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error stopping VPS: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Restart VPS", style=discord.ButtonStyle.blurple)
    async def restart_vps(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.response.defer(ephemeral=True)
            
            try:
                container = bot.docker_client.containers.get(self.container_id)
            except docker.errors.NotFound:
                await self.handle_missing_container(interaction)
                return
            
            token, vps = bot.db.get_vps_by_id(self.vps_id)
            if vps['status'] == 'suspended':
                await interaction.followup.send("❌ This VPS is suspended. Contact admin to unsuspend.", ephemeral=True)
                return

            container.restart()
            await asyncio.sleep(5)
            
            # Update restart count in VPS data
            if token:
                updates = {
                    'restart_count': vps.get('restart_count', 0) + 1,
                    'last_restart': str(datetime.datetime.now()),
                    'status': 'running'
                }
                bot.db.update_vps(token, updates)
                
                bot.db.increment_stat('total_restarts')
                
                # Get new SSH session
                try:
                    exec_cmd = await asyncio.create_subprocess_exec(
                        "docker", "exec", self.container_id, "tmate", "-F",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )

                    ssh_session_line = await capture_ssh_session_line(exec_cmd)
                    if ssh_session_line:
                        bot.db.update_vps(token, {'tmate_session': ssh_session_line})
                        
                        # Send new SSH details to owner
                        try:
                            owner = await bot.fetch_user(int(vps["created_by"]))
                            embed = discord.Embed(title=f"HostForge VPS Restarted - {self.vps_id}", color=discord.Color.blue())
                            embed.add_field(name="New SSH Session", value=f"```{ssh_session_line}```", inline=False)
                            embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
                            await owner.send(embed=embed)
                        except:
                            pass
                except:
                    pass
            
            embed = discord.Embed(title=f"HostForge VPS Management - {self.vps_id}", color=discord.Color.green())
            embed.add_field(name="Status", value="🟢 Running", inline=True)
            
            if vps:
                embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
                embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
                embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
                embed.add_field(name="Username", value=vps['username'], inline=True)
                embed.add_field(name="Created", value=vps['created_at'], inline=True)
                embed.add_field(name="Restart Count", value=vps.get('restart_count', 0) + 1, inline=True)
            
            await interaction.message.edit(embed=embed, view=VPSManagementView(self.vps_id, container.id))
            await interaction.followup.send("✅ HostForge VPS restarted successfully! New SSH details sent to owner.", ephemeral=True)
        except Exception as e:
            await interaction.followup.send(f"❌ Error restarting VPS: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Reinstall OS", style=discord.ButtonStyle.grey)
    async def reinstall_os(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            try:
                container = bot.docker_client.containers.get(self.container_id)
            except docker.errors.NotFound:
                await self.handle_missing_container(interaction)
                return
            
            view = OSSelectionView(self.vps_id, self.container_id, interaction.message)
            await interaction.response.send_message("Select new OS:", view=view, ephemeral=True)
        except Exception as e:
            await interaction.response.send_message(f"❌ Error: {str(e)}", ephemeral=True)

    @discord.ui.button(label="Transfer VPS", style=discord.ButtonStyle.grey)
    async def transfer_vps(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = TransferVPSModal(self.vps_id)
        await interaction.response.send_modal(modal)

class OSSelectionView(ui.View):
    def __init__(self, vps_id, container_id, original_message):
        super().__init__(timeout=300)
        self.vps_id = vps_id
        self.container_id = container_id
        self.original_message = original_message
        
        self.add_os_button("Ubuntu 22.04", "ubuntu:22.04")
        self.add_os_button("Debian 12", "debian:12")
        self.add_os_button("Arch Linux", "archlinux:latest")
        self.add_os_button("Alpine", "alpine:latest")
        self.add_os_button("CentOS 7", "centos:7")
        self.add_os_button("Fedora 38", "fedora:38")

    def add_os_button(self, label: str, image: str):
        button = discord.ui.Button(label=label, style=discord.ButtonStyle.grey)
        
        async def os_callback(interaction: discord.Interaction):
            await self.reinstall_os(interaction, image)
        
        button.callback = os_callback
        self.add_item(button)

    async def reinstall_os(self, interaction: discord.Interaction, image: str):
        try:
            token, vps = bot.db.get_vps_by_id(self.vps_id)
            if not vps:
                await interaction.response.send_message("❌ VPS not found!", ephemeral=True)
                return

            await interaction.response.defer(ephemeral=True)

            try:
                old_container = bot.docker_client.containers.get(self.container_id)
                old_container.stop()
                old_container.remove()
            except Exception as e:
                logger.error(f"Error removing old container: {e}")

            status_msg = await interaction.followup.send("🔄 Reinstalling HostForge VPS... This may take a few minutes.", ephemeral=True)
            
            memory_bytes = vps['memory'] * 1024 * 1024 * 1024

            try:
                container = bot.docker_client.containers.run(
                    image,
                    detach=True,
                    privileged=True,
                    hostname=f"hostforge-{self.vps_id}",
                    mem_limit=memory_bytes,
                    cpu_period=100000,
                    cpu_quota=int(vps['cpu'] * 100000),
                    cap_add=["ALL"],
                    command="tail -f /dev/null",
                    tty=True,
                    network=DOCKER_NETWORK,
                    ports={'22/tcp': str(vps['external_ssh_port'])},
                    volumes={
                        f'hostforge-{self.vps_id}': {'bind': '/data', 'mode': 'rw'}
                    }
                )
            except docker.errors.ImageNotFound:
                await status_msg.edit(content=f"❌ OS image {image} not found. Using default {DEFAULT_OS_IMAGE}")
                container = bot.docker_client.containers.run(
                    DEFAULT_OS_IMAGE,
                    detach=True,
                    privileged=True,
                    hostname=f"hostforge-{self.vps_id}",
                    mem_limit=memory_bytes,
                    cpu_period=100000,
                    cpu_quota=int(vps['cpu'] * 100000),
                    cap_add=["ALL"],
                    command="tail -f /dev/null",
                    tty=True,
                    network=DOCKER_NETWORK,
                    ports={'22/tcp': str(vps['external_ssh_port'])},
                    volumes={
                        f'hostforge-{self.vps_id}': {'bind': '/data', 'mode': 'rw'}
                    }
                )
                image = DEFAULT_OS_IMAGE

            bot.db.update_vps(token, {
                'container_id': container.id,
                'os_image': image
            })

            try:
                setup_success, _ = await setup_container(
                    container.id, 
                    status_msg, 
                    vps['memory'], 
                    vps_id=self.vps_id
                )
                if not setup_success:
                    raise Exception("Failed to setup container")
                
                bot.db.update_vps(token, {'password': "root"})
            except Exception as e:
                await status_msg.edit(content=f"❌ Container setup failed: {str(e)}")
                return

            try:
                exec_cmd = await asyncio.create_subprocess_exec(
                    "docker", "exec", container.id, "tmate", "-F",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )

                ssh_session_line = await capture_ssh_session_line(exec_cmd)
                if ssh_session_line:
                    bot.db.update_vps(token, {'tmate_session': ssh_session_line})
                    
                    # Send new SSH details to owner
                    try:
                        owner = await bot.fetch_user(int(vps["created_by"]))
                        embed = discord.Embed(title=f"HostForge VPS Reinstalled - {self.vps_id}", color=discord.Color.blue())
                        embed.add_field(name="New OS", value=image, inline=True)
                        embed.add_field(name="New SSH Session", value=f"```{ssh_session_line}```", inline=False)
                        embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
                        embed.add_field(name="Password", value=f"||root||", inline=False)
                        await owner.send(embed=embed)
                    except:
                        pass
            except Exception as e:
                logger.error(f"Warning: Failed to start tmate session: {e}")

            await status_msg.edit(content="✅ HostForge VPS reinstalled successfully!")
            
            try:
                embed = discord.Embed(title=f"HostForge VPS Management - {self.vps_id}", color=discord.Color.green())
                embed.add_field(name="Status", value="🟢 Running", inline=True)
                embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
                embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
                embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
                embed.add_field(name="Username", value="root", inline=True)
                embed.add_field(name="Created", value=vps['created_at'], inline=True)
                embed.add_field(name="OS", value=image, inline=True)
                
                await self.original_message.edit(embed=embed, view=VPSManagementView(self.vps_id, container.id))
            except Exception as e:
                logger.error(f"Warning: Failed to update original message: {e}")

        except Exception as e:
            try:
                await interaction.followup.send(f"❌ Error reinstalling VPS: {str(e)}", ephemeral=True)
            except:
                try:
                    channel = interaction.channel
                    await channel.send(f"❌ Error reinstalling HostForge VPS {self.vps_id}: {str(e)}")
                except:
                    logger.error(f"Failed to send error message: {e}")

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True
        try:
            await self.original_message.edit(view=self)
        except:
            pass

class TransferVPSModal(ui.Modal, title='Transfer VPS'):
    def __init__(self, vps_id: str):
        super().__init__()
        self.vps_id = vps_id
        self.new_owner = ui.TextInput(
            label='New Owner',
            placeholder='Enter user ID or @mention',
            required=True
        )
        self.add_item(self.new_owner)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            new_owner_input = self.new_owner.value.strip()
            
            # Extract user ID from mention if provided
            if new_owner_input.startswith('<@') and new_owner_input.endswith('>'):
                new_owner_id = new_owner_input[2:-1]
                if new_owner_id.startswith('!'):  # Handle nickname mentions
                    new_owner_id = new_owner_id[1:]
            else:
                # Validate it's a numeric ID
                if not new_owner_input.isdigit():
                    await interaction.response.send_message("❌ Please provide a valid user ID or @mention", ephemeral=True)
                    return
                new_owner_id = new_owner_input

            token, vps = bot.db.get_vps_by_id(self.vps_id)
            if not vps or vps["created_by"] != str(interaction.user.id):
                await interaction.response.send_message("❌ VPS not found or you don't have permission to transfer it!", ephemeral=True)
                return

            try:
                old_owner = await bot.fetch_user(int(vps["created_by"]))
                old_owner_name = old_owner.name
            except:
                old_owner_name = "Unknown User"

            try:
                new_owner = await bot.fetch_user(int(new_owner_id))
                new_owner_name = new_owner.name
                
                # Check if new owner is banned
                if bot.db.is_user_banned(new_owner.id):
                    await interaction.response.send_message(f"❌ {new_owner.mention} is banned!", ephemeral=True)
                    return

                # Check if new owner already has max VPS
                if bot.db.get_user_vps_count(new_owner.id) >= bot.db.get_setting('max_vps_per_user'):
                    await interaction.response.send_message(f"❌ {new_owner.mention} already has the maximum number of VPS instances ({bot.db.get_setting('max_vps_per_user')})", ephemeral=True)
                    return
            except:
                await interaction.response.send_message("❌ Invalid user ID or mention!", ephemeral=True)
                return

            bot.db.update_vps(token, {"created_by": str(new_owner.id)})

            await interaction.response.send_message(f"✅ HostForge VPS {self.vps_id} has been transferred from {old_owner_name} to {new_owner_name}!", ephemeral=True)
            
            try:
                embed = discord.Embed(title="HostForge VPS Transferred to You", color=discord.Color.green())
                embed.add_field(name="VPS ID", value=self.vps_id, inline=True)
                embed.add_field(name="Previous Owner", value=old_owner_name, inline=True)
                embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
                embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
                embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
                embed.add_field(name="Username", value="root", inline=True)
                embed.add_field(name="Access Token", value=token, inline=False)
                embed.add_field(name="SSH Password", value=f"||root||", inline=False)
                embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
                await new_owner.send(embed=embed)
            except:
                await interaction.followup.send("Note: Could not send DM to the new owner.", ephemeral=True)

        except Exception as e:
            logger.error(f"Error in TransferVPSModal: {e}")
            await interaction.response.send_message(f"❌ Error transferring VPS: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='manage_vps', description='Manage a VPS instance')
@app_commands.describe(
    vps_id="ID of the VPS to manage"
)
async def manage_vps(ctx, vps_id: str):
    """Manage a VPS instance"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or (vps["created_by"] != str(ctx.author.id) and not has_admin_role(ctx)):
            await ctx.send("❌ VPS not found or you don't have access to it!", ephemeral=True)
            return

        try:
            container = bot.docker_client.containers.get(vps["container_id"])
            container_status = container.status.capitalize()
        except:
            container_status = "Not Found"

        status = vps['status'].capitalize()

        embed = discord.Embed(title=f"HostForge VPS Management - {vps_id}", color=discord.Color.blue())
        embed.add_field(name="Status", value=f"{status} (Container: {container_status})", inline=True)
        embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
        embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
        embed.add_field(name="Disk Allocated", value=f"{vps['disk']}GB", inline=True)
        embed.add_field(name="Username", value="root", inline=True)
        embed.add_field(name="Created", value=vps['created_at'], inline=True)
        embed.add_field(name="OS", value=vps.get('os_image', DEFAULT_OS_IMAGE), inline=True)
        embed.add_field(name="Restart Count", value=vps.get('restart_count', 0), inline=True)
        embed.add_field(name="SSH Port", value=vps['external_ssh_port'], inline=True)

        view = VPSManagementView(vps_id, vps["container_id"])
        
        message = await ctx.send(embed=embed, view=view)
        view.original_message = message
    except Exception as e:
        logger.error(f"Error in manage_vps: {e}")
        await ctx.send(f"❌ Error managing VPS: {str(e)}", ephemeral=True)

@bot.hybrid_command(name='transfer_vps', description='Transfer a VPS to another user')
@app_commands.describe(
    vps_id="ID of the VPS to transfer",
    new_owner="User to transfer the VPS to"
)
async def transfer_vps_command(ctx, vps_id: str, new_owner: discord.Member):
    """Transfer a VPS to another user"""
    try:
        token, vps = bot.db.get_vps_by_id(vps_id)
        if not vps or vps["created_by"] != str(ctx.author.id):
            await ctx.send("❌ VPS not found or you don't have permission to transfer it!", ephemeral=True)
            return

        if bot.db.is_user_banned(new_owner.id):
            await ctx.send("❌ This user is banned!", ephemeral=True)
            return

        # Check if new owner already has max VPS
        if bot.db.get_user_vps_count(new_owner.id) >= bot.db.get_setting('max_vps_per_user'):
            await ctx.send(f"❌ {new_owner.mention} already has the maximum number of VPS instances ({bot.db.get_setting('max_vps_per_user')})", ephemeral=True)
            return

        bot.db.update_vps(token, {"created_by": str(new_owner.id)})

        await ctx.send(f"✅ HostForge VPS {vps_id} has been transferred from {ctx.author.name} to {new_owner.name}!")

        try:
            embed = discord.Embed(title="HostForge VPS Transferred to You", color=discord.Color.green())
            embed.add_field(name="VPS ID", value=vps_id, inline=True)
            embed.add_field(name="Previous Owner", value=ctx.author.name, inline=True)
            embed.add_field(name="Memory", value=f"{vps['memory']}GB", inline=True)
            embed.add_field(name="CPU", value=f"{vps['cpu']} cores", inline=True)
            embed.add_field(name="Disk", value=f"{vps['disk']}GB", inline=True)
            embed.add_field(name="Username", value="root", inline=True)
            embed.add_field(name="Access Token", value=token, inline=False)
            embed.add_field(name="SSH Password", value=f"||root||", inline=False)
            embed.add_field(name="Direct SSH", value=f"```ssh root@{bot.public_ip} -p {vps['external_ssh_port']}```", inline=False)
            await new_owner.send(embed=embed)
        except:
            await ctx.send("Note: Could not send DM to the new owner.", ephemeral=True)

    except Exception as e:
        logger.error(f"Error in transfer_vps_command: {e}")
        await ctx.send(f"❌ Error transferring VPS: {str(e)}", ephemeral=True)

@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("❌ You don't have permission to use this command!", ephemeral=True)
    elif isinstance(error, commands.CommandNotFound):
        await ctx.send("❌ Command not found! Use `/help` to see available commands.", ephemeral=True)
    elif isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(f"❌ Missing required argument: {error.param.name}", ephemeral=True)
    else:
        logger.error(f"Command error: {error}")
        await ctx.send(f"❌ An error occurred: {str(error)}", ephemeral=True)

# Run the bot
if __name__ == "__main__":
    try:
        # Create directories if they don't exist
        os.makedirs("temp_dockerfiles", exist_ok=True)
        os.makedirs("migrations", exist_ok=True)
        
        bot.run(TOKEN)
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
        traceback.print_exc()