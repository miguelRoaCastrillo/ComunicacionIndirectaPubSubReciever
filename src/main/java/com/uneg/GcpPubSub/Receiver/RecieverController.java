package com.uneg.GcpPubSub.Receiver;

import com.google.api.gax.paging.Page;
import com.google.auth.appengine.AppEngineCredentials;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.BaseServiceException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.Lists;
import com.google.pubsub.v1.ProjectName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.Topic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@RestController
public class RecieverController {

    @Value("${ID_PROYECTO}")
    private String idProyecto;

    @Value("${RUTA_CREDENCIALES}")
    private String rutaCredenciales;

    /**
     * Lista todos los temas del proyecto de GPC PUB/SUB
     * @return
     * @throws IOException
     */
    @GetMapping("/ListarTemas")
    public ArrayList<String> listarTemas() throws IOException {

        ArrayList<String> listaTemas = new ArrayList<>();

        //Para encontrar todos los temas de google cloud
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            ProjectName projectName = ProjectName.of(idProyecto);
            for (Topic topic : topicAdminClient.listTopics(projectName).iterateAll()) {
                System.out.println(topic.getName());
                listaTemas.add(topic.getName());
            }

            return listaTemas;
        }catch(Exception error){
            System.out.println("Existe un error al tomar la lista de temas: " + error.toString());
            return (ArrayList) null;
        }
    }

    @PostMapping("/RecibirMensaje")
    public void recibirMensajes(String idSuscripcion){

        Subscriber subscriber = null;

        try{
            ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(idProyecto, idSuscripcion);

            // Instancia un recibirdor de mensajes asincrono
            MessageReceiver receiver =
                    new MessageReceiver() {
                        @Override
                        public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                            // Maneja mensajes entrantes
                            System.out.println("Id: " + message.getMessageId());
                            System.out.println("Data: " + message.getData().toStringUtf8());
                            System.out.println("Intentos de envío: " + Subscriber.getDeliveryAttempt(message));
                            consumer.ack();
                        }
                    };

            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            // Inicia el suscriptor
            subscriber.startAsync().awaitRunning();
            System.out.printf("Escuchando para mensajes en %s:\n", subscriptionName.toString());
            // Permite al suscriptor para correr por 60s, a menos que ocurrean errores irrecuperables
            subscriber.awaitTerminated(60, TimeUnit.SECONDS);

        }catch(Exception error){
            System.out.println("Existe un error al momento de recibir los mensajes: " + error.toString());
            // Apaga el suscriptor despues de 30, el deja de recibir mensajes
            subscriber.stopAsync();
        }
    }
}
