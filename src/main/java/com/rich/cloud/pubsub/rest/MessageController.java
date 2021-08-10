package com.rich.cloud.pubsub.rest;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.spring.pubsub.PubSubAdmin;
import com.google.cloud.spring.pubsub.core.PubSubTemplate;
import com.google.cloud.spring.pubsub.support.AcknowledgeablePubsubMessage;
import com.google.common.collect.ImmutableMap;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.view.RedirectView;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

@Slf4j
@RestController
public class MessageController {

    private final PubSubTemplate pubSubTemplate;

    private final PubSubAdmin pubSubAdmin;

    private final ArrayList<Subscriber> allSubscribers;

    public MessageController(PubSubTemplate pubSubTemplate, PubSubAdmin pubSubAdmin) {
        this.pubSubTemplate = pubSubTemplate;
        this.pubSubAdmin = pubSubAdmin;
        this.allSubscribers = new ArrayList<>();
    }

    @PostMapping("/createTopic")
    public RedirectView createTopic(@RequestParam("topicName") String topicName) {
        this.pubSubAdmin.createTopic(topicName);

        return buildAdminStatusView("Topic creation successful.");
    }

    @PostMapping("/createSubscription")
    public RedirectView createSubscription(@RequestParam("topicName") String topicName,
                                           @RequestParam("subscriptionName") String subscriptionName) {
        this.pubSubAdmin.createSubscription(subscriptionName, topicName);

        return buildAdminStatusView("Subscription creation successful.");
    }

    @GetMapping("/postMessage")
    public RedirectView publish(@RequestParam("topicName") String topicName,
                                @RequestParam("message") String message, @RequestParam("count") int messageCount) {
        for (int i = 0; i < messageCount; i++) {
            this.pubSubTemplate.publish(topicName, message);
        }

        return buildStatusView("Messages published asynchronously.");
    }

    @GetMapping("/pull")
    public RedirectView pull(@RequestParam("subscription1") String subscriptionName) {

        Collection<AcknowledgeablePubsubMessage> messages = this.pubSubTemplate.pull(subscriptionName, 10, true);

        if (messages.isEmpty()) {
            return buildStatusView("No messages available for retrieval.");
        }

        RedirectView returnView;
        try {
            ListenableFuture<Void> ackFuture = this.pubSubTemplate.ack(messages);
            ackFuture.get();
            messages.stream().
                    map(AcknowledgeablePubsubMessage::getPubsubMessage).
                    forEach(m -> log.info("MessageId: {}, data: {}", m.getMessageId(), m.getData().toStringUtf8()));
            returnView = buildStatusView(String.format("Pulled and acked %s message(s)", messages.size()));
        } catch (Exception ex) {
            log.warn("Acking failed.", ex);
            returnView = buildStatusView("Acking failed");
        }

        return returnView;
    }

    @GetMapping("/multipull")
    public RedirectView multipull(
            @RequestParam("subscription1") String subscriptionName1,
            @RequestParam("subscription2") String subscriptionName2) {

        Set<AcknowledgeablePubsubMessage> mixedSubscriptionMessages = new HashSet<>();
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName1, 1000, true));
        mixedSubscriptionMessages.addAll(this.pubSubTemplate.pull(subscriptionName2, 1000, true));

        if (mixedSubscriptionMessages.isEmpty()) {
            return buildStatusView("No messages available for retrieval.");
        }

        RedirectView returnView;
        try {
            ListenableFuture<Void> ackFuture = this.pubSubTemplate.ack(mixedSubscriptionMessages);
            ackFuture.get();
            returnView = buildStatusView(
                    String.format("Pulled and acked %s message(s)", mixedSubscriptionMessages.size()));
        } catch (Exception ex) {
            log.warn("Acking failed.", ex);
            returnView = buildStatusView("Acking failed");
        }

        return returnView;
    }

    @GetMapping("/subscribe")
    public RedirectView subscribe(@RequestParam("subscription") String subscriptionName) {
        Subscriber subscriber = this.pubSubTemplate.subscribe(subscriptionName, (message) -> {
            log.info("Message received from {} subscription: {}",
                    subscriptionName, message.getPubsubMessage().getData().toStringUtf8());
            message.ack();
        });

        this.allSubscribers.add(subscriber);
        return buildStatusView("Subscribed.");
    }

    @PostMapping("/deleteTopic")
    public RedirectView deleteTopic(@RequestParam("topic") String topicName) {
        this.pubSubAdmin.deleteTopic(topicName);

        return buildAdminStatusView("Topic deleted successfully.");
    }

    @PostMapping("/deleteSubscription")
    public RedirectView deleteSubscription(@RequestParam("subscription") String subscriptionName) {
        this.pubSubAdmin.deleteSubscription(subscriptionName);

        return buildAdminStatusView("Subscription deleted successfully.");
    }

    private RedirectView buildStatusView(String statusMessage) {
        RedirectView view = new RedirectView("/push-pull-messages.html");
        view.addStaticAttribute("statusMessage", statusMessage);
        return view;
    }

    private RedirectView buildAdminStatusView(String statusMessage) {
        RedirectView view = new RedirectView("/admin-index.html");
        view.addStaticAttribute("statusMessage", statusMessage);
        return view;
    }

}
