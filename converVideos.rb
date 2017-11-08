require 'rubygems'
require 'aws-sdk'
require 'open-uri'

class ConexionSQS
	def initialize
		@sqs = Aws::SQS::Client.new(region: 'us-east-2')
		@queue_url = "https://sqs.us-east-2.amazonaws.com/232651884417/conversionvideos"
	end
	def LeerCola
		@resp = @sqs.receive_message(queue_url: @queue_url, max_number_of_messages: 5)
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
		@s32 = Aws::S3::Client.new(region: 'us-east-1')
        end
	def ObtenerVideoOriginal rutaS3
		rutaArch = rutaS3.split('proyecto3').last.split('/').last
		File.open(rutaArch, 'wb') do |file|
  			file << open(rutaS3).read
		end
		
	end
	def SubirVideoConvert archivoConvert
		File.open(archivoConvert, "r") do |aFile|
            		@s32.put_object(bucket: 'p4videos-originales', key: archivoConvert, body: aFile)
        	end
	end
	
	def SubirVideoSinConvert archivoOrigi, archivoConvert
                File.open(archivoOrigi, "r") do |aFile|
                        @s32.put_object(bucket: 'p4videos-convertidos', key: archivoConvert, body: aFile)
                end
        end
end

def EliminaArchivos arcVideoOri
	system('rm ' + arcVideoOri)
end

def ConversionVideo arcVideoOri,arcVideoConv
	puts arcVideoOri + ' ' + arcVideoConv
	presetId = '1351620000001-100070' # ID for the sytem web preset
	elastictranscoder = Aws::ElasticTranscoder::Client.new(region: 'us-east-1')
	pipelineId =  '1510106586284-5xnwhp'
	job_options = {}
	job_options[:pipeline_id] = pipelineId
	job_options[:input] = {
		key: arcVideoOri
	}
	job_options[:output] = {
		key: arcVideoConv,
		preset_id: presetId,
		thumbnail_pattern: "{count}-#{arcVideoOri}"
	}
	job = elastictranscoder.create_job(job_options)
end

def NotificaEmail email,nombre
	encoding  = 'UTF-8'
	textbody = 'el video ha sido publicado'
	subject  = 'video publicado'
	sender = 'sa.melo@uniandes.edu.co' 
	message = <<MESSAGE_END
<b>El video ha sido publicado.</b>
<h1>por favor visite la pagina del consurso.</h1>
MESSAGE_END
	puts message
	ses = Aws::SES::Client.new(region: 'us-east-1')
	resp = ses.send_email({
    		destination: {
    			to_addresses: [
      				email,
      			],
    		},
    		message: {
      			body: {
        			html: {
          			charset: encoding,
          			data: message,
        			},
        		text: {
          			charset: encoding,
          			data: textbody,
        			},
      			},
    			subject: {
      				charset: encoding,
      				data: subject,
    			},
  		},
  		source: sender,
  		})	



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
        arcVideoConv = rutaorigi.split('proyecto3').last.split('/').last + '_convert.mp4'
	puts "extencion " + arcVideo.split('.').last
	if arcVideo.split('.').last == "mp4"
		s3.SubirVideoSinConvert arcVideo,arcVideoConv
	else
		s3.SubirVideoConvert arcVideo
                ConversionVideo arcVideo,arcVideoConv
	end
	dynamo.ActualizarVideoCovertido m.body.to_s,"https://s3.amazonaws.com/p4videos-convertidos/" + arcVideoConv
	NotificaEmail correo,nombre
	EliminaArchivos arcVideo
	sqs.ElimnarMensaje m
end

