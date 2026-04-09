# 使用官方 Python 镜像作为基础镜像
FROM python:3.9-slim

# 设置工作目录
WORKDIR /app

# 复制 requirements.txt 并安装依赖
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 设置环境变量，从 .env 文件加载（在运行时通过 docker-compose 处理）
# 注意：Dockerfile 中不直接加载 .env，.env 由 docker-compose 处理

# 暴露端口（如果需要）
# EXPOSE 8000

# 定义启动命令
CMD ["python", "hero_route_diff_rebuild.py"]