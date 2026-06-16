import os
import uuid
import glob
import json
import subprocess
import shlex
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from flask import Flask, request, jsonify, send_file, render_template, Response
import boto3

from cookie_harvester import start_harvester, COOKIE_FILE

app = Flask(__name__)
DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

def get_s3_client():
    bucket = os.environ.get("S3_BUCKET")
    access_key = os.environ.get("S3_ACCESS_KEY_ID")
    secret_key = os.environ.get("S3_SECRET_ACCESS_KEY")
    if not bucket or not access_key or not secret_key:
        return None
    
    region = os.environ.get("S3_REGION")
    endpoint_url = os.environ.get("S3_ENDPOINT_URL")
    
    kwargs = {
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
    }
    if region:
        kwargs["region_name"] = region
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
        
    return boto3.client("s3", **kwargs)


jobs = {}
download_executor = ThreadPoolExecutor(max_workers=5)


def cleanup_old_jobs():
    while True:
        try:
            now = time.time()
            cutoff = now - 86400  # 24 hours

            # 1. Clean up jobs dictionary and their files
            expired_job_ids = []
            for job_id, job in list(jobs.items()):
                created_at = job.get("created_at")
                if created_at and created_at < cutoff:
                    expired_job_ids.append(job_id)

            for job_id in expired_job_ids:
                job = jobs.pop(job_id, None)
                if job and "file" in job:
                    try:
                        if os.path.exists(job["file"]):
                            os.remove(job["file"])
                    except OSError:
                        pass
                
                meta_file = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
                s3_uploaded = False
                if os.path.exists(meta_file):
                    try:
                        with open(meta_file, "r", encoding="utf-8") as f:
                            meta_data_json = json.load(f)
                            s3_uploaded = meta_data_json.get("s3_uploaded", False)
                    except Exception:
                        pass
                
                if not s3_uploaded:
                    try:
                        if os.path.exists(meta_file):
                            os.remove(meta_file)
                    except OSError:
                        pass

            # 2. Clean up any orphaned files in DOWNLOAD_DIR older than 24h
            for filename in os.listdir(DOWNLOAD_DIR):
                file_path = os.path.join(DOWNLOAD_DIR, filename)
                if os.path.isfile(file_path):
                    try:
                        mtime = os.path.getmtime(file_path)
                        if mtime < cutoff:
                            if filename.endswith(".txt"):
                                s3_uploaded = False
                                try:
                                    with open(file_path, "r", encoding="utf-8") as f:
                                        meta_data_json = json.load(f)
                                        s3_uploaded = meta_data_json.get("s3_uploaded", False)
                                except Exception:
                                    pass
                                if s3_uploaded:
                                    continue
                            os.remove(file_path)
                    except OSError:
                        pass
        except Exception:
            pass

        time.sleep(3600)  # Check every hour


cleanup_thread = threading.Thread(target=cleanup_old_jobs, daemon=True)
cleanup_thread.start()

start_harvester()


def run_download(job_id, url, format_choice, format_id):
    job = jobs[job_id]
    job["status"] = "downloading"

    # Capture fresh metadata via yt-dlp -j
    uploader = "Unknown"
    title = job.get("title", "")
    try:
        info_cmd = ["yt-dlp", "--no-playlist", "-j"]
        if os.path.exists(COOKIE_FILE):
            info_cmd.extend(["--cookies", COOKIE_FILE])
        info_cmd.append(url)
        info_res = subprocess.run(info_cmd, capture_output=True, text=True, timeout=30)
        if info_res.returncode == 0:
            info_data = json.loads(info_res.stdout)
            title = info_data.get("title", title)
            uploader = info_data.get("uploader", "Unknown")
            job["title"] = title
            job["uploader"] = uploader
    except Exception:
        pass

    # Save initial metadata file
    meta_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
    meta_data = {
        "job_id": job_id,
        "url": url,
        "title": title,
        "uploader": uploader,
        "format_choice": format_choice,
        "status": "downloading",
        "created_at": job.get("created_at", time.time())
    }

    def save_meta():
        try:
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump(meta_data, f, indent=2)
        except Exception:
            pass

    save_meta()

    out_template = os.path.join(DOWNLOAD_DIR, f"{job_id}.%(ext)s")

    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--newline",
        "--progress-template",
        "download-progress:%(progress.downloaded_bytes)s/%(progress.total_bytes)s/%(progress.total_bytes_estimate)s/%(progress.speed)s/%(progress.eta)s",
        "-o",
        out_template
    ]

    use_aria2c = os.environ.get("USE_ARIA2C", "").lower() == "true"
    if use_aria2c:
        threads = os.environ.get("USE_ARIA2C_THREADS", "8")
        cmd.extend([
            "--downloader",
            "aria2c",
            "--downloader-args",
            f"aria2c:-j {threads} -x {threads} -s {threads} -k 1M"
        ])

    if os.path.exists(COOKIE_FILE):
        cmd.extend(["--cookies", COOKIE_FILE])

    if format_choice == "audio":
        cmd += ["-x", "--audio-format", "mp3"]
    elif format_id:
        cmd += ["-f", f"{format_id}+bestaudio/best", "--merge-output-format", "mp4"]
    else:
        cmd += ["-f", "bestvideo+bestaudio/best", "--merge-output-format", "mp4"]

    cmd.append(url)

    meta_data["command"] = shlex.join(cmd)
    save_meta()

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )

        output_lines = []

        def read_output():
            for line in iter(process.stdout.readline, ""):
                print(f"[yt-dlp {job_id}] {line}", end="", flush=True)
                output_lines.append(line)

                # Parse progress lines
                if line.startswith("download-progress:"):
                    try:
                        payload = line.strip().split(":", 1)[1]
                        fields = payload.split("/")
                        downloaded = int(float(fields[0]))
                        total = int(float(fields[1])) if fields[1] not in ("NA", "None", "null") else None
                        estimate = int(float(fields[2])) if fields[2] not in ("NA", "None", "null") else None
                        speed = float(fields[3]) if fields[3] not in ("NA", "None", "null") else None
                        eta = int(float(fields[4])) if fields[4] not in ("NA", "None", "null") else None

                        total_bytes = total if total is not None else estimate
                        percent = (downloaded / total_bytes * 100) if total_bytes else None

                        job["progress"] = {
                            "percent": round(percent, 1) if percent is not None else None,
                            "downloaded_mb": round(downloaded / (1024 * 1024), 2),
                            "total_mb": round(total_bytes / (1024 * 1024), 2) if total_bytes else None,
                            "speed_mbps": round(speed / (1024 * 1024), 2) if speed else None,
                            "eta_seconds": eta
                        }
                    except (IndexError, ValueError):
                        pass

        reader_thread = threading.Thread(target=read_output, daemon=True)
        reader_thread.start()

        try:
            return_code = process.wait(timeout=300)
        except subprocess.TimeoutExpired:
            process.kill()
            reader_thread.join(timeout=5)
            raise

        reader_thread.join(timeout=5)

        if return_code != 0:
            job["status"] = "error"
            last_line = ""
            for line in reversed(output_lines):
                cleaned = line.strip()
                if cleaned:
                    last_line = cleaned
                    break
            job["error"] = last_line or f"yt-dlp exited with code {return_code}"

            meta_data["status"] = "error"
            meta_data["error"] = job["error"]
            save_meta()
            return

        files = glob.glob(os.path.join(DOWNLOAD_DIR, f"{job_id}.*"))
        if not files:
            job["status"] = "error"
            job["error"] = "Download completed but no file was found"

            meta_data["status"] = "error"
            meta_data["error"] = job["error"]
            save_meta()
            return

        if format_choice == "audio":
            target = [f for f in files if f.endswith(".mp3")]
            chosen = target[0] if target else files[0]
        else:
            target = [f for f in files if f.endswith(".mp4")]
            chosen = target[0] if target else files[0]

        for f in files:
            if f != chosen:
                try:
                    os.remove(f)
                except OSError:
                    pass

        job["status"] = "done"
        job["file"] = chosen
        ext = os.path.splitext(chosen)[1]
        title = job.get("title", "").strip()
        # Sanitize title for filename
        if title:
            safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:20].strip()
            job["filename"] = f"{safe_title}{ext}" if safe_title else os.path.basename(chosen)
        else:
            job["filename"] = os.path.basename(chosen)

        meta_data["status"] = "done"
        meta_data["filename"] = job["filename"]
        save_meta()

        # Upload to S3 if configured
        s3_client = get_s3_client()
        if s3_client:
            bucket = os.environ.get("S3_BUCKET")
            prefix = os.environ.get("S3_PREFIX", "")
            media_key = f"{prefix}{job_id}{ext}"
            meta_key = f"{prefix}{job_id}.txt"
            try:
                print(f"[S3 Upload {job_id}] Starting upload of media file {chosen} to S3 bucket {bucket} key {media_key}...", flush=True)
                s3_client.upload_file(chosen, bucket, media_key)
                print(f"[S3 Upload {job_id}] Media upload completed successfully.", flush=True)

                # Add S3 flags to metadata
                meta_data["s3_uploaded"] = True
                save_meta()

                print(f"[S3 Upload {job_id}] Starting upload of metadata {meta_path} to S3 bucket {bucket} key {meta_key}...", flush=True)
                s3_client.upload_file(meta_path, bucket, meta_key)
                print(f"[S3 Upload {job_id}] Metadata upload completed successfully.", flush=True)
            except Exception as s3_err:
                print(f"[S3 Upload {job_id}] Error uploading to S3: {s3_err}", flush=True)
                meta_data["s3_upload_error"] = str(s3_err)
                save_meta()

    except subprocess.TimeoutExpired:

        job["status"] = "error"
        job["error"] = "Download timed out (5 min limit)"

        meta_data["status"] = "error"
        meta_data["error"] = job["error"]
        save_meta()
    except Exception as e:
        job["status"] = "error"
        job["error"] = str(e)

        meta_data["status"] = "error"
        meta_data["error"] = job["error"]
        save_meta()


@app.route("/")
def index():
    instance_name = os.environ.get("INSTANCE_NAME", "")
    return render_template("index.html", instance_name=instance_name)


@app.route("/api/info", methods=["POST"])
def get_info():
    data = request.json
    url = data.get("url", "").strip()
    if not url:
        return jsonify({"error": "No URL provided"}), 400

    cmd = ["yt-dlp", "--no-playlist", "-j"]
    if os.path.exists(COOKIE_FILE):
        cmd.extend(["--cookies", COOKIE_FILE])
    cmd.append(url)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode != 0:
            return jsonify({"error": result.stderr.strip().split("\n")[-1]}), 400

        info = json.loads(result.stdout)

        # Build quality options — keep best format per resolution
        best_by_height = {}
        for f in info.get("formats", []):
            height = f.get("height")
            if height and f.get("vcodec", "none") != "none":
                tbr = f.get("tbr") or 0
                if height not in best_by_height or tbr > (best_by_height[height].get("tbr") or 0):
                    best_by_height[height] = f

        formats = []
        for height, f in best_by_height.items():
            formats.append({
                "id": f["format_id"],
                "label": f"{height}p",
                "height": height,
            })
        formats.sort(key=lambda x: x["height"], reverse=True)

        return jsonify({
            "title": info.get("title", ""),
            "thumbnail": info.get("thumbnail", ""),
            "duration": info.get("duration"),
            "uploader": info.get("uploader", ""),
            "formats": formats,
        })
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timed out fetching video info"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/download", methods=["POST"])
def start_download():
    data = request.json
    url = data.get("url", "").strip()
    format_choice = data.get("format", "video")
    format_id = data.get("format_id")
    title = data.get("title", "")

    if not url:
        return jsonify({"error": "No URL provided"}), 400

    job_id = uuid.uuid4().hex[:10]
    created_at = time.time()
    
    jobs[job_id] = {
        "status": "queued",
        "url": url,
        "title": title,
        "format_choice": format_choice,
        "created_at": created_at,
    }

    # Save initial metadata file as queued so list_downloads reads it correctly immediately
    try:
        meta_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
        meta_data = {
            "job_id": job_id,
            "url": url,
            "title": title,
            "uploader": "Unknown",
            "format_choice": format_choice,
            "status": "queued",
            "created_at": created_at
        }
        with open(meta_path, "w", encoding="utf-8") as f:
            json.dump(meta_data, f, indent=2)
    except Exception:
        pass

    # Submit job to the ThreadPoolExecutor queue
    download_executor.submit(run_download, job_id, url, format_choice, format_id)

    return jsonify({"job_id": job_id})


@app.route("/api/status/<job_id>")
def check_status(job_id):
    job = jobs.get(job_id)
    if not job:
        # Fallback to check if a metadata file exists (in case of server restart)
        meta_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
        if os.path.exists(meta_path):
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                return jsonify({
                    "status": meta.get("status"),
                    "error": meta.get("error"),
                    "filename": meta.get("filename"),
                })
            except Exception:
                pass
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status": job["status"],
        "error": job.get("error"),
        "filename": job.get("filename"),
        "progress": job.get("progress"),
    })


@app.route("/api/downloads")
def list_downloads():
    results = []
    if not os.path.exists(DOWNLOAD_DIR):
        return jsonify(results)

    # Fetch object list from S3 if configured
    s3_client = get_s3_client()
    s3_metadata_keys = {}
    s3_media_keys = {}
    bucket = os.environ.get("S3_BUCKET")
    prefix = os.environ.get("S3_PREFIX", "")

    if s3_client and bucket:
        try:
            paginator = s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    rel_name = key[len(prefix):] if key.startswith(prefix) else key
                    if rel_name.endswith(".txt"):
                        job_id = rel_name[:-4]
                        s3_metadata_keys[job_id] = key
                    else:
                        base, ext = os.path.splitext(rel_name)
                        if ext and ext != ".part" and ext != ".ytdl":
                            s3_media_keys[base] = key
        except Exception as e:
            print(f"[S3 List] Error listing S3 objects: {e}", flush=True)

        # Sync missing metadata files from S3 to local cache
        for job_id, key in s3_metadata_keys.items():
            local_meta_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
            if not os.path.exists(local_meta_path):
                try:
                    print(f"[S3 Sync] Caching missing metadata locally for {job_id} from S3 key {key}...", flush=True)
                    s3_client.download_file(bucket, key, local_meta_path)
                except Exception as e:
                    print(f"[S3 Sync] Error downloading metadata key {key}: {e}", flush=True)

    for filename in os.listdir(DOWNLOAD_DIR):
        if filename.endswith(".txt"):
            meta_path = os.path.join(DOWNLOAD_DIR, filename)
            job_id = filename[:-4]
            try:
                with open(meta_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
            except Exception:
                continue

            # Verify the status dynamically based on files in DOWNLOAD_DIR
            media_files = []
            part_files = []
            for f in os.listdir(DOWNLOAD_DIR):
                if f.startswith(job_id) and f != filename:
                    if f.endswith(".part") or f.endswith(".ytdl"):
                        part_files.append(f)
                    else:
                        media_files.append(f)

            # Determine storage locations
            storage = []
            if media_files:
                storage.append("local")
            if job_id in s3_media_keys:
                storage.append("s3")

            meta["storage"] = storage

            s3_media_exists = job_id in s3_media_keys
            s3_media_key = s3_media_keys.get(job_id)

            active_job = jobs.get(job_id)
            if active_job:
                meta["status"] = active_job["status"]
                if "error" in active_job:
                    meta["error"] = active_job["error"]
                if "progress" in active_job:
                    meta["progress"] = active_job["progress"]
            elif media_files or s3_media_exists:
                meta["status"] = "done"
                # If filename is missing, reconstruct it
                if not meta.get("filename"):
                    ext = os.path.splitext(media_files[0])[1] if media_files else os.path.splitext(s3_media_key)[1]
                    title = meta.get("title", "").strip()
                    if title:
                        safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:20].strip()
                        meta["filename"] = f"{safe_title}{ext}" if safe_title else f"{job_id}{ext}"
                    else:
                        meta["filename"] = f"{job_id}{ext}"
            elif part_files:
                meta["status"] = "downloading"
            else:
                # If metadata says "downloading" or "queued" but job is not in memory and no files are found, mark error
                if meta.get("status") in ("downloading", "queued"):
                    meta["status"] = "error"
                    meta["error"] = "Interrupted or failed"

            results.append(meta)

    # Sort results by created_at descending (newest first)
    results.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    return jsonify(results)



@app.route("/api/file/<job_id>")
def download_file(job_id):
    # 1. Try to load metadata
    meta = None
    meta_path = os.path.join(DOWNLOAD_DIR, f"{job_id}.txt")
    if os.path.exists(meta_path):
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            pass

    # 2. Check if a local media file exists
    local_media_file = None
    if os.path.exists(DOWNLOAD_DIR):
        for f in os.listdir(DOWNLOAD_DIR):
            if f.startswith(job_id) and f != f"{job_id}.txt":
                if not (f.endswith(".part") or f.endswith(".ytdl")):
                    local_media_file = os.path.join(DOWNLOAD_DIR, f)
                    break

    # 3. If local media file exists, serve it
    if local_media_file:
        filename = None
        if meta:
            filename = meta.get("filename")
        if not filename:
            ext = os.path.splitext(local_media_file)[1]
            title = meta.get("title", "").strip() if meta else ""
            if title:
                safe_title = "".join(c for c in title if c not in r'\/:*?"<>|').strip()[:20].strip()
                filename = f"{safe_title}{ext}" if safe_title else os.path.basename(local_media_file)
            else:
                filename = os.path.basename(local_media_file)
        return send_file(local_media_file, as_attachment=True, download_name=filename)

    # 4. If local file is missing, try streaming from S3 if configured
    s3_client = get_s3_client()
    if s3_client:
        bucket = os.environ.get("S3_BUCKET")
        prefix = os.environ.get("S3_PREFIX", "")
        
        try:
            res = s3_client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}{job_id}.")
            media_key = None
            for obj in res.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".txt"):
                    media_key = key
                    break
            
            if media_key:
                filename = None
                if meta:
                    filename = meta.get("filename")
                if not filename:
                    filename = os.path.basename(media_key)
                
                # Stream the file chunk by chunk to avoid high RAM usage
                def generate():
                    resp = s3_client.get_object(Bucket=bucket, Key=media_key)
                    for chunk in resp["Body"].iter_chunks(chunk_size=1024*1024):
                        yield chunk
                
                return Response(
                    generate(),
                    headers={
                        "Content-Disposition": f'attachment; filename="{filename}"',
                        "Content-Type": "application/octet-stream"
                    }
                )
        except Exception as e:
            print(f"[S3 Download] Error streaming file {job_id} from S3: {e}", flush=True)

    # 5. Check active job status in memory as fallback
    job = jobs.get(job_id)
    if job and job["status"] == "done" and "file" in job and os.path.exists(job["file"]):
        return send_file(job["file"], as_attachment=True, download_name=job["filename"])

    return jsonify({"error": "File not found or not ready"}), 404



if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8899))
    host = os.environ.get("HOST", "127.0.0.1")
    app.run(host=host, port=port)
