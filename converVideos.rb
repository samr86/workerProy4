require 'streamio-ffmpeg'
require 'rubygems'
require 'net/smtp'
require 'aws-sdk'
require 'aws-sdk-sqs'
require 'open-uri'

class ConexionSQS
	def initialize
		@sqs = Aws::SQS::Client.new(region: 'us-east-2')
		@queue_url = "https://sqs.us-east-2.amazonaws.com/232651884417/conversionvideos"
	end
	def LeerCola
		@resp = @sqs.receive_message(queue_url: @queue_url, max_number_of_messages: 1)
	end
	def Invisibilizar mensaje
		@sqs.change_message_visibility({
     			queue_url: @queue_url,
      			receipt_handle: mensaje.receipt_handle,
      			visibility_timeout: 30 })		
    	end
	def ElimnarMensaje mensaje
		@resp = @sqs.delete_message(queue_url: @queue_url, receipt_handle: mensaje.receipt_handle)
	end
end


class ConexionDB
	@@videoOri
	@@email
	@@nombre
	def initialize
		@dynamoDB = Aws::DynamoDB::Client.new(region: 'us-east-2')
	end
	def getvideoOri
		@@videoOri
	end
	def getemail
		@@email
	end
	def getnombre
		@@nombre
	end
	def ObtenerDatosVideos uuid
		key = { uuid: uuid}
		params = {
    			table_name: 'Videos',
    			key: {uuid: uuid}
		}
		begin
  			result = @dynamoDB.get_item(params)
			@@videoOri = result.item['videoOriginal'].to_s
			@@email = result.item['email'].to_s
			@@nombre = result.item['nombres'].to_s			
		rescue  Aws::DynamoDB::Errors::ServiceError => error
  			puts 'Unable to find movie:'
			puts error.message
		end
	end
	def ActualizarVideoCovertido uuid,rutas3
		key = { uuid: uuid}
                params = {
                        table_name: 'Videos',
                        key: {uuid: uuid},
			update_expression: "set videoNuevo = :r, estado = :p",
			expression_attribute_values: {
       				":r" => rutas3,
        			":p" => "convertido"
   	 		},
    			return_values: "UPDATED_NEW"
                	}
                begin
                        result = @dynamoDB.update_item(params)
                rescue  Aws::DynamoDB::Errors::ServiceError => error
                        puts 'Unable to find movie:'
                        puts error.message
                end
              
	end
end

class ConexionS3
	def initialize
                @s3 = Aws::S3::Client.new(region: 'us-east-2')
        end
	def ObtenerVideoOriginal rutaS3
		rutaArch = rutaS3.split('proyecto3').last.split('/').last
		File.open(rutaArch, 'wb') do |file|
  			file << open(rutaS3).read
		end
		
	end
	def SubirVideoConvert archivoConvert
		File.open(archivoConvert, "r") do |aFile|
            		@s3.put_object(bucket: 'proyecto3', key: archivoConvert, body: aFile)
        	end
	end
end

def EliminaArchivos arcVideoOri,arcVideoConv
	system('rm ' + arcVideoOri)
	system('rm ' + arcVideoConv)
end

def ConversionVideo arcVideoOri,arcVideoConv
	puts arcVideoOri + ' ' + arcVideoConv
	video = FFMPEG::Movie.new(arcVideoOri)
	puts video.valid?
	puts video.video_codec
	puts video.audio_codec.to_s
	if video.valid?
		puts "se pudo leer el video y se procede a convertir: " + video.video_codec + ' ' + video.audio_codec.to_s
		if video.video_codec == "h264" and video.audio_codec.to_s == "aac"
			system('cp ' + arcVideoOri + ' ' + arcVideoConv)
			@fileSize = video.size
		else 
			#options = {video_codec: "h264",audio_codec: "aac"}
			options = {video_codec: "libx264",audio_codec: "aac"}
                        options = {video_codec: "libx264",audio_codec: "mp3"}
			#transcoder_options = { validate: false }
			#options = {video_codec: "h264"}
			video.transcode(arcVideoConv, options) { |progress| puts progress }
			videoConv = FFMPEG::Movie.new(arcVideoConv)
			if videoConv.valid?
				puts "se pudo leer convertido"
			end
			@fileSize = videoConv.size
		end
	else
		puts "video no se pudo leer"
	end
end

def NotificaEmail email,nombre
	message = <<MESSAGE_END
From: ConcursoVideos <cloudgrupo3@gmail.com>
To: #{nombre}  <#{email}>
MIME-Version: 1.0
Content-type: text/html
Subject: Video Publicado
<b>El video ha sido publicado.</b>
<h1>por favor visite la pagina del consurso.</h1>
MESSAGE_END
	puts message

	server = 'smtp.gmail.com'
	mail_from_domain = 'gmail.com'
	port = 587      # or 25 - double check with your provider
	username = 'cloudgrupo3@gmail.com'
	password = 'grupo3clouduniandes'
	smtp = Net::SMTP.new(server, port)
	smtp.enable_starttls_auto
	smtp.start(server,username,password, :plain)
	smtp.send_message(message,'ConcursoVideos',email)
end

puts "INICIO DEL PROGRAMA DE CONVERSION"
n = rand(1..5)
puts "sleep de " + n.to_s + "segundos"
sleep(n)
sqs = ConexionSQS.new
dynamo = ConexionDB.new
s3 = ConexionS3.new
mensajes = sqs.LeerCola
mensajes.messages.each do |m|
	sqs.Invisibilizar m  
	puts m.body
	dynamo.ObtenerDatosVideos m.body.to_s
	rutaorigi =  dynamo.getvideoOri
	correo = dynamo.getemail
	nombre = dynamo.getnombre
	s3.ObtenerVideoOriginal rutaorigi
	arcVideo = rutaorigi.split('proyecto3').last.split('/').last
        arcVideoConv = rutaorigi.split('proyecto3').last.split('/').last + '_concert.mp4'
	puts "obtuvo video original"
	ConversionVideo arcVideo,arcVideoConv 
	s3.SubirVideoConvert arcVideoConv
	dynamo.ActualizarVideoCovertido m.body.to_s,"https://s3-us-east-2.amazonaws.com/proyecto3/" + arcVideoConv
	NotificaEmail correo,nombre
	EliminaArchivos arcVideo,arcVideoConv
	sqs.ElimnarMensaje m
end

