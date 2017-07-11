package nl.rabobank.paf.balance;

import nl.rabobank.beb.client.kafka.streams.TopologyFactory;
import nl.rabobank.beb.distribution.CustomerAlertSettings;
import nl.rabobank.beb.distribution.CustomerId;
import nl.rabobank.beb.distribution.CustomerIds;
import nl.rabobank.beb.distribution.EmailAddress;
import nl.rabobank.beb.distribution.OutboundMessage;
import nl.rabobank.beb.distribution.PhoneNumber;
import nl.rabobank.beb.payments.AccountEntry;
import nl.rabobank.beb.payments.AccountId;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.javatuples.Pair;

import java.util.Collections;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;

public class BalanceAlertsTopology implements TopologyFactory {
    @Override
    public void createTopology(KStreamBuilder builder) {
        final String accountEntryStream = "accountentry";
        final String accountToCustomerIdsTable = "accounttocustomerids";
        final String alertSettingsTable = "customeralertsettings";
        final String customerIdToAccountEntryStream = "customeraccountentry";
        final String emailMessageStream = "outboundemailmessage";
        final String smsMessageStream = "outboundsmsmessage";
        final String customerPushMessageStream = "outboundcustomerpushmessage";

        final KTable<AccountId, CustomerIds> accountToCustomerIds = builder.table(accountToCustomerIdsTable, accountToCustomerIdsTable);
        final KTable<CustomerId, CustomerAlertSettings> alertSettings = builder.table(alertSettingsTable, alertSettingsTable);

        /*
        Transform accountEntries to addressedMessages with the following steps:
            * Enrich with accountId -> CustomerId
            * Enrich with CustomerId -> AlertSettings
            * Address and generate messages using the Mapper
         */
        KStream<CustomerId, KeyValue<SpecificRecord, OutboundMessage>> addressedMessages =
                builder.<AccountId, AccountEntry>stream(accountEntryStream)
                        .leftJoin(accountToCustomerIds, (accountEntry, customerIds) -> {
                            if (isNull(customerIds)) {
                                return Collections.<KeyValue<CustomerId, AccountEntry>>emptyList();
                            } else {
                                return customerIds.getCustomerIds().stream()
                                        .map(customerId -> KeyValue.pair(customerId, accountEntry))
                                        .collect(toList());
                            }
                        })
                        .flatMap((accountId, accountentryByCustomer) -> accountentryByCustomer)
                        .through(customerIdToAccountEntryStream)
                        .leftJoin(alertSettings, Pair::with)
                        .flatMapValues(
                                (Pair<AccountEntry, CustomerAlertSettings> accountEntryAndSettings) ->
                                        BalanceAlertsGenerator.generateAlerts(
                                                accountEntryAndSettings.getValue0(),
                                                accountEntryAndSettings.getValue1())
                        );

        // Send all Email messages from addressedMessages
        addressedMessages
                .filter((e, kv) -> kv.key instanceof EmailAddress)
                .map((k, v) -> v)
                .to(emailMessageStream);

        // Send all Sms messages from addressedMessages
        addressedMessages
                .filter((e, kv) -> kv.key instanceof PhoneNumber)
                .map((k, v) -> v)
                .to(smsMessageStream);

        // Send all Push messages from addressedMessages
        // (CustomerId is later resolved to a list of customer's mobile devices)
        addressedMessages
                .filter((e, kv) -> kv.key instanceof CustomerId)
                .map((k, v) -> v)
                .to(customerPushMessageStream);
    }
}
