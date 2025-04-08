package com.pulsar.common.common_kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@EmbeddedKafka(partitions = 1, topics = "test-topic")
@ContextConfiguration(classes = KafkaTestConfig.class)
public class KafkaIntegrationTest {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    @Test
    void devePublicarMensagemNoKafka() {
        // Arrange
        String topico = "test-topic";
        String chave = "123";
        TestMessage mensagem = new TestMessage("abc", 42); // objeto de exemplo (POJO)

        kafkaTemplate.send(topico, chave, mensagem);

        // Act
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("grupo-teste", "true", embeddedKafka);
        var consumerFactory = new DefaultKafkaConsumerFactory<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        var consumer = consumerFactory.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(consumer, topico);

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, topico);

        // Assert
        assertEquals(chave, record.key());
        assertTrue(record.value().contains("\"id\":\"abc\"")); // ou use ObjectMapper para comparar JSON
    }

    // Classe interna de exemplo (POJO)
    static class TestMessage {
        private String id;
        private int numero;

        public TestMessage() {}

        public TestMessage(String id, int numero) {
            this.id = id;
            this.numero = numero;
        }

        public String getId() { return id; }
        public int getNumero() { return numero; }
    }
}
