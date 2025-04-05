import os
import re
import uuid
import ffmpeg

from kombu import Queue
from flask import Flask
from celery import Celery

from src.config import Config
from src.s3_client import S3Client
from src.rabbitmq_client import RabbitMQClient
from src.file_client import FileClient
from src.converter import ProtobufConverter
from src.Protobuf.Message_pb2 import Clip, VideoFormatStyle, ClipStatus, VideoFormatterMessage, Video

app = Flask(__name__)
app.config.from_object(Config)
s3_client = S3Client(Config)
rmq_client = RabbitMQClient()
file_client = FileClient()

celery = Celery("tasks", broker=app.config["RABBITMQ_URL"])

celery.conf.update(
    {
        "task_serializer": "json",
        "accept_content": ["json"],
        "broker_connection_retry_on_startup": True,
        "task_routes": {
            "tasks.process_message": {"queue": app.config["RMQ_QUEUE_WRITE"]}
        },
        "task_queues": [
            Queue(
                app.config["RMQ_QUEUE_READ"], routing_key=app.config["RMQ_QUEUE_READ"]
            )
        ],
    }
)

@celery.task(name="tasks.process_message", queue=app.config["RMQ_QUEUE_READ"])
def process_message(message):
    clip: Clip = ProtobufConverter.json_to_protobuf(message)

    try:
        id = clip.id
        type = os.path.splitext(clip.originalVideo.name)[1]

        keyVideo = f"{clip.userId}/{clip.id}/{clip.originalVideo.name}"
        keyProcessedVideo = f"{clip.userId}/{clip.id}/{id}_processed{type}"

        tmpVideoPath = f"/tmp/{clip.originalVideo.name}"
        tmpProcessedVideoPath = f"/tmp/{id}_processed{type}"

        if not s3_client.download_file(keyVideo, tmpVideoPath):
            raise Exception()

        if clip.configuration.format == VideoFormatStyle.Name(VideoFormatStyle.ORIGINAL):
            if not s3_client.upload_file(tmpVideoPath, keyProcessedVideo):
                raise Exception()
        
        if clip.configuration.format == VideoFormatStyle.Name(VideoFormatStyle.ZOOMED_916):
            if not convert_to_zoomed_916(tmpVideoPath, tmpProcessedVideoPath):
                raise Exception()
            if not s3_client.upload_file(tmpProcessedVideoPath, keyProcessedVideo):
                raise Exception()
            
        file_client.delete_file(tmpProcessedVideoPath)
        file_client.delete_file(tmpVideoPath)
        
        processed_video = create_processed_video(
            clip.originalVideo, f"{id}_processed{type}"
        )

        clip.processedVideo.CopyFrom(processed_video)

        clip.status = ClipStatus.Name(
            ClipStatus.VIDEO_FORMATTER_COMPLETE
        )

        protobuf = VideoFormatterMessage()
        protobuf.clip.CopyFrom(clip)

        if not rmq_client.send_message(protobuf, "App\\Protobuf\\VideoFormatterMessage"):
            raise Exception()
        
        return True
        
    except Exception:
        clip.status = ClipStatus.Name(
            ClipStatus.VIDEO_FORMATTER_ERROR
        )

        protobuf = VideoFormatterMessage()
        protobuf.clip.CopyFrom(clip)

        if not rmq_client.send_message(protobuf, "App\\Protobuf\\VideoFormatterMessage"):
            return False
        
def convert_to_zoomed_916(input_video, output_video):
    probe = ffmpeg.probe(input_video)
    video_stream = next(
        (stream for stream in probe["streams"] if stream["codec_type"] == "video"), None
    )

    if not video_stream:
        raise ValueError("video_stream error on parameters.")

    width = int(video_stream["width"])
    height = int(video_stream["height"])
    new_width = int((9 / 16) * height)
    crop_x = max(0, (width - new_width) // 2)

    ffmpeg.input(input_video).filter("crop", new_width, height, crop_x, 0).output(
        output_video,
        vcodec="libx264",
        crf=23,
        preset="fast",
        acodec="aac",
        audio_bitrate="128k",
        map="0:a",
    ).run()

    return True

def create_processed_video(video: Video, name: str) -> Video:
    processed_video = Video()
    processed_video.id = str(uuid.uuid4())
    processed_video.name = name
    processed_video.mimeType = video.mimeType
    processed_video.size = video.size

    if video.originalName:
        processed_video.originalName = video.originalName

    if video.length:
        processed_video.length = video.length

    if video.ass:
        processed_video.ass = video.ass

    if video.subtitle:
        processed_video.subtitle = video.subtitle

    processed_video.IsInitialized()

    return processed_video
