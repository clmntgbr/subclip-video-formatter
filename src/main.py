import os
import re
import tempfile
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
    file_client.delete_all_files_in_folder('/tmp')

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
        
        if clip.configuration.format == VideoFormatStyle.Name(VideoFormatStyle.NORMAL_916_WITH_BORDERS):
            if not convert_normal_916_with_borders(tmpVideoPath, tmpProcessedVideoPath):
                raise Exception()
            if not s3_client.upload_file(tmpProcessedVideoPath, keyProcessedVideo):
                raise Exception()
        
        if clip.configuration.format == VideoFormatStyle.Name(VideoFormatStyle.DUPLICATED_BLURRED_916):
            if not convert_normal_916_with_blur(tmpVideoPath, tmpProcessedVideoPath):
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

def make_even(x):
    return x if x % 2 == 0 else x - 1

def convert_normal_916_with_blur(input_video, output_video, blur_strength=20):
    temp_dir = tempfile.mkdtemp()
    temp_blurred = os.path.join(temp_dir, "temp_blurred.mp4")
    temp_original = os.path.join(temp_dir, "temp_original.mp4")
    
    try:
        # Analyser la vidéo d'entrée
        probe = ffmpeg.probe(input_video)
        video_stream = next(
            (stream for stream in probe["streams"] if stream["codec_type"] == "video"), None
        )
        
        if not video_stream:
            raise ValueError("Erreur: aucun flux vidéo trouvé.")
        
        # Récupérer les dimensions originales
        original_width = int(video_stream["width"])
        original_height = int(video_stream["height"])
        
        # Calculer les dimensions pour le format 9:16 de TikTok
        target_ratio = 9 / 16
        current_ratio = original_width / original_height
        
        if current_ratio > target_ratio:
            # La vidéo est trop large
            # Conserver la largeur et augmenter la hauteur
            tiktok_height = int(original_width / target_ratio)
            # S'assurer que la hauteur est divisible par 2
            if tiktok_height % 2 != 0:
                tiktok_height += 1
            tiktok_width = original_width
            
            # Calculer les positions pour le centrage
            y_pad = (tiktok_height - original_height) // 2
            y_pad = y_pad if y_pad % 2 == 0 else y_pad + 1  # S'assurer que y_pad est pair
            
            # Créer une version floutée, étirée et recadrée pour l'arrière-plan
            # Étape 1: Créer une version floutée pleine taille
            (
                ffmpeg
                .input(input_video)
                .filter('scale', tiktok_width, tiktok_height, force_original_aspect_ratio='increase')
                .filter('crop', tiktok_width, tiktok_height)
                .filter('boxblur', blur_strength, 5)
                .output(temp_blurred, vcodec='libx264', crf=23, preset='fast')
                .overwrite_output()
                .run(quiet=True)
            )
            
            # Étape 2: Préparer la vidéo originale
            (
                ffmpeg
                .input(input_video)
                .output(temp_original, vcodec='libx264', crf=23, preset='fast')
                .overwrite_output()
                .run(quiet=True)
            )
            
            # Étape 3: Superposer la vidéo originale sur la version floutée
            (
                ffmpeg
                .input(temp_blurred)
                .overlay(
                    ffmpeg.input(temp_original),
                    x=0,
                    y=y_pad
                )
                .output(
                    output_video,
                    vcodec='libx264',
                    crf=23,
                    preset='fast',
                    acodec='aac',
                    audio_bitrate='128k',
                )
                .overwrite_output()
                .run()
            )
            
        else:
            # La vidéo est trop haute ou déjà au format 9:16
            # Calculer la largeur pour respecter le ratio 9:16
            tiktok_width = int(original_height * target_ratio)
            # S'assurer que la largeur est divisible par 2
            if tiktok_width % 2 != 0:
                tiktok_width += 1
            tiktok_height = original_height
            
            # Calculer les positions pour le centrage
            x_pad = (tiktok_width - original_width) // 2
            x_pad = x_pad if x_pad % 2 == 0 else x_pad + 1  # S'assurer que x_pad est pair
            
            # Créer une version floutée, étirée et recadrée pour l'arrière-plan
            # Étape 1: Créer une version floutée pleine taille
            (
                ffmpeg
                .input(input_video)
                .filter('scale', tiktok_width, tiktok_height, force_original_aspect_ratio='increase')
                .filter('crop', tiktok_width, tiktok_height)
                .filter('boxblur', blur_strength, 5)
                .output(temp_blurred, vcodec='libx264', crf=23, preset='fast')
                .overwrite_output()
                .run(quiet=True)
            )
            
            # Étape 2: Préparer la vidéo originale
            (
                ffmpeg
                .input(input_video)
                .output(temp_original, vcodec='libx264', crf=23, preset='fast')
                .overwrite_output()
                .run(quiet=True)
            )
            
            # Étape 3: Superposer la vidéo originale sur la version floutée
            (
                ffmpeg
                .input(temp_blurred)
                .overlay(
                    ffmpeg.input(temp_original),
                    x=x_pad,
                    y=0
                )
                .output(
                    output_video,
                    vcodec='libx264',
                    crf=23,
                    preset='fast',
                    acodec='aac',
                    audio_bitrate='128k',
                )
                .overwrite_output()
                .run()
            )
            
    finally:
        # Nettoyer les fichiers temporaires
        try:
            if os.path.exists(temp_blurred):
                os.remove(temp_blurred)
            if os.path.exists(temp_original):
                os.remove(temp_original)
            os.rmdir(temp_dir)
        except:
            pass
    
    return True

def convert_normal_916_with_borders(input_video, output_video, padding_color="black"):
    probe = ffmpeg.probe(input_video)
    video_stream = next(
        (stream for stream in probe["streams"] if stream["codec_type"] == "video"), None
    )
    
    if not video_stream:
        raise ValueError("Erreur: aucun flux vidéo trouvé.")
    
    original_width = int(video_stream["width"])
    original_height = int(video_stream["height"])
    
    target_ratio = 9 / 16
    
    new_height = int(original_width / target_ratio)
    
    pad_top = (new_height - original_height) // 2
    pad_bottom = new_height - original_height - pad_top
    
    if original_width / original_height < target_ratio:
        target_width = int(original_height * target_ratio)
        
        ffmpeg.input(input_video).filter("scale", target_width, -1).filter("pad", 
                width=target_width, 
                height=int(target_width / target_ratio), 
                x=0, 
                y="(oh-ih)/2",
                color=padding_color).output(
            output_video,
            vcodec="libx264",
            crf=23,
            preset="fast",
            acodec="aac", 
            audio_bitrate="128k",
            map="0:a"
        ).run()
    else:
        ffmpeg.input(input_video).filter("pad", 
                width=original_width, 
                height=new_height, 
                x=0, 
                y=pad_top, 
                color=padding_color).output(
            output_video,
            vcodec="libx264",
            crf=23,
            preset="fast",
            acodec="aac", 
            audio_bitrate="128k",
            map="0:a"
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
