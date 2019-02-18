import io

from google.cloud.texttospeech_v1 import TextToSpeechClient
from google.cloud.texttospeech_v1.proto.cloud_tts_pb2 import MP3


from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

credentials = GoogleCloudBaseHook()._get_credentials()

input_ = {"text": "co tam stary? testujemy polski j"}

voice = {"language_code": "pl-PL"}

audio_config = {"audio_encoding": "MP3"}

client = TextToSpeechClient(credentials=credentials)

res = client.synthesize_speech(input_, voice, audio_config)
audio = res.audio_content

file = open("./text-to-speech-output.mp3", "wb")
file.write(audio)
file.close()

print(res)
