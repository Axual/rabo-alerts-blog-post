package nl.rabobank.paf.balance;

import nl.rabobank.beb.distribution.ChannelType;
import nl.rabobank.beb.distribution.CustomerAccountAlertSetting;
import nl.rabobank.beb.distribution.CustomerAccountAlertSettings;
import nl.rabobank.beb.distribution.CustomerAlertAddress;
import nl.rabobank.beb.distribution.CustomerAlertSettings;
import nl.rabobank.beb.distribution.CustomerId;
import nl.rabobank.beb.distribution.EmailAddress;
import nl.rabobank.beb.distribution.OutboundMessage;
import nl.rabobank.beb.distribution.PhoneNumber;
import nl.rabobank.beb.payments.AccountEntry;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.javatuples.Pair;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class BalanceAlertsGenerator {
    private static final String DEBIT = "DBIT";
    private static final String CREDIT = "CRDT";
    private static final String ALERT_BALANCE_ABOVE_THRESHOLD = "ALERT_BALANCE_ABOVE_THRESHOLD";
    private static final String ALERT_BALANCE_BELOW_THRESHOLD = "ALERT_BALANCE_BELOW_THRESHOLD";
    private static final String ALERT_DEBITED_ABOVE_THRESHOLD = "ALERT_DEBITED_ABOVE_THRESHOLD";
    private static final String ALERT_CREDITED_ABOVE_THRESHOLD = "ALERT_CREDITED_ABOVE_THRESHOLD";

    private BalanceAlertsGenerator() {
    }

    public static List<KeyValue<SpecificRecord, OutboundMessage>> generateAlerts(AccountEntry accountEntry,
                                                                                 CustomerAlertSettings settings) {
        /* Generates addressed alerts for an AccountEntry, using the alert settings with the following steps:
        *  1) Settings are for a specific account, drop AccountEntries not for this account
        *  2) Match each setting with all alerts to generate appropriate messages
        *  3) Address the generated messages
        */

        if (settings == null) {
            return new ArrayList<>();
        }

        return settings.getAccountAlertSettings().stream()
                .filter(accountAlertSettings -> matchAccount(accountEntry, accountAlertSettings))
                .flatMap(accountAlertSettings -> accountAlertSettings.getSettings().stream())
                .flatMap(accountAlertSetting -> Stream.of(
                        generateBalanceAbove(accountEntry, accountAlertSetting),
                        generateBalanceBelow(accountEntry, accountAlertSetting),
                        generateCreditedAbove(accountEntry, accountAlertSetting),
                        generateDebitedAbove(accountEntry, accountAlertSetting))
                )
                .filter(Optional::isPresent).map(Optional::get)
                .flatMap(messageWithChannels -> mapAddresses(messageWithChannels.getValue0(), settings.getAddresses())
                        .map(address -> KeyValue.pair(address, messageWithChannels.getValue1())))
                .collect(toList());
    }

    private static boolean matchAccount(AccountEntry accountEntry, CustomerAccountAlertSettings settings) {
        return Objects.equals(settings.getAccount().getId(), accountEntry.getAccountId())
                && Objects.equals(settings.getCurrency().getSymbol(), accountEntry.getAccountCurrency());

    }

    private static Optional<Pair<List<ChannelType>, OutboundMessage>> generateBalanceAbove(AccountEntry accountEntry,
                                                                                           CustomerAccountAlertSetting setting) {
        if (Objects.equals(setting.getAlertType(), ALERT_BALANCE_ABOVE_THRESHOLD)) {
            final String balanceSign = Objects.equals(accountEntry.getBalanceAfterBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
            final BigDecimal balance = new BigDecimal(accountEntry.getBalanceAfterBookingNumeric());
            final BigDecimal balanceFormatted = new BigDecimal(balanceSign + accountEntry.getBalanceAfterBooking());
            final String amountSign = Objects.equals(accountEntry.getBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
            final BigDecimal amount = new BigDecimal(amountSign + String.valueOf(accountEntry.getBookingAmountNumeric()));
            final BigDecimal balanceBefore = balance.subtract(amount);
            final BigDecimal threshold = new BigDecimal(setting.getAmount().getValue());

            if (balance.compareTo(threshold) > 0
                    && balanceBefore.compareTo(threshold) <= 0) {
                final OutboundMessage message = buildMessage(ALERT_BALANCE_ABOVE_THRESHOLD, Stream.of(
                        Pair.with("alert_settings_account_number", accountEntry.getAccountId()),
                        Pair.with("alert_settings_account_account", accountEntry.getAccountCurrency()),
                        Pair.with("alert_settings_amount", threshold.movePointLeft(2).toString()),
                        Pair.with("alert_settings_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("balance_amount", balanceFormatted.toString()),
                        Pair.with("balance_amount_currency", accountEntry.getAccountCurrency())));

                return Optional.of(Pair.with(setting.getChannels(), message));
            }
        }

        return Optional.empty();
    }

    private static Optional<Pair<List<ChannelType>, OutboundMessage>> generateBalanceBelow(AccountEntry accountEntry,
                                                                                           CustomerAccountAlertSetting setting) {
        if (Objects.equals(setting.getAlertType(), ALERT_BALANCE_BELOW_THRESHOLD)) {
            final String balanceSign = Objects.equals(accountEntry.getBalanceAfterBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
            final BigDecimal balance = new BigDecimal(accountEntry.getBalanceAfterBookingNumeric());
            final BigDecimal balanceFormatted = new BigDecimal(balanceSign + accountEntry.getBalanceAfterBooking());
            final String amountSign = Objects.equals(accountEntry.getBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
            final BigDecimal amount = new BigDecimal(amountSign + String.valueOf(accountEntry.getBookingAmountNumeric()));
            final BigDecimal balanceBefore = balance.subtract(amount);
            final BigDecimal threshold = new BigDecimal(setting.getAmount().getValue());

            if (balance.compareTo(threshold) < 0
                    && balanceBefore.compareTo(threshold) >= 0) {
                final OutboundMessage message = buildMessage(ALERT_BALANCE_BELOW_THRESHOLD, Stream.of(
                        Pair.with("alert_settings_account_number", accountEntry.getAccountId()),
                        Pair.with("alert_settings_account_account", accountEntry.getAccountCurrency()),
                        Pair.with("alert_settings_amount", threshold.movePointLeft(2).toString()),
                        Pair.with("alert_settings_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("balance_amount", balanceFormatted.toString()),
                        Pair.with("balance_amount_currency", accountEntry.getAccountCurrency())));

                return Optional.of(Pair.with(setting.getChannels(), message));
            }
        }

        return Optional.empty();
    }

    private static Optional<Pair<List<ChannelType>, OutboundMessage>> generateDebitedAbove(AccountEntry accountEntry,
                                                                                           CustomerAccountAlertSetting setting) {
        if (Objects.equals(setting.getAlertType(), ALERT_DEBITED_ABOVE_THRESHOLD)) {
            final BigDecimal amountFormatted = new BigDecimal(accountEntry.getBookingAmount());
            final BigDecimal amount = new BigDecimal(accountEntry.getBookingAmountNumeric());
            final BigDecimal threshold = new BigDecimal(setting.getAmount().getValue());

            if (Objects.equals(accountEntry.getBookingCreditDebitIndicator(), DEBIT)
                    && amount.compareTo(threshold) > 0) {
                final String balanceSign = Objects.equals(accountEntry.getBalanceAfterBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
                final BigDecimal balanceFormatted = new BigDecimal(balanceSign + accountEntry.getBalanceAfterBooking());

                final OutboundMessage message = buildMessage(ALERT_DEBITED_ABOVE_THRESHOLD, Stream.of(
                        Pair.with("alert_settings_account_number", accountEntry.getAccountId()),
                        Pair.with("alert_settings_account_account", accountEntry.getAccountCurrency()),
                        Pair.with("alert_settings_amount", threshold.movePointLeft(2).toString()),
                        Pair.with("alert_settings_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("balance_amount", balanceFormatted.toString()),
                        Pair.with("balance_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("alert_amount_amount", amountFormatted.toString()),
                        Pair.with("alert_amount_amount_currency", accountEntry.getAccountCurrency())));

                return Optional.of(Pair.with(setting.getChannels(), message));
            }
        }

        return Optional.empty();
    }

    private static Optional<Pair<List<ChannelType>, OutboundMessage>> generateCreditedAbove(AccountEntry accountEntry,
                                                                                            CustomerAccountAlertSetting setting) {
        if (Objects.equals(setting.getAlertType(), ALERT_CREDITED_ABOVE_THRESHOLD)) {
            final BigDecimal amountFormatted = new BigDecimal(accountEntry.getBookingAmount());
            final BigDecimal amount = new BigDecimal(accountEntry.getBookingAmountNumeric());
            final BigDecimal threshold = new BigDecimal(setting.getAmount().getValue());

            if (Objects.equals(accountEntry.getBookingCreditDebitIndicator(), CREDIT)
                    && amount.compareTo(threshold) > 0) {
                final String balanceSign = Objects.equals(accountEntry.getBalanceAfterBookingCreditDebitIndicator(), DEBIT) ? "-" : "";
                final BigDecimal balanceFormatted = new BigDecimal(balanceSign + accountEntry.getBalanceAfterBooking());

                final OutboundMessage message = buildMessage(ALERT_CREDITED_ABOVE_THRESHOLD, Stream.of(
                        Pair.with("alert_settings_account_number", accountEntry.getAccountId()),
                        Pair.with("alert_settings_account_account", accountEntry.getAccountCurrency()),
                        Pair.with("alert_settings_amount", threshold.movePointLeft(2).toString()),
                        Pair.with("alert_settings_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("balance_amount", balanceFormatted.toString()),
                        Pair.with("balance_amount_currency", accountEntry.getAccountCurrency()),
                        Pair.with("alert_amount_amount", amountFormatted.toString()),
                        Pair.with("alert_amount_amount_currency", accountEntry.getAccountCurrency())));

                return Optional.of(Pair.with(setting.getChannels(), message));
            }
        }

        return Optional.empty();
    }

    private static Stream<SpecificRecord> mapAddresses(List<ChannelType> channels, List<CustomerAlertAddress> addresses) {
        return addresses.stream()
                .map(CustomerAlertAddress::getAddress)
                .filter(address -> address instanceof EmailAddress && channels.contains(ChannelType.EMAIL)
                        || address instanceof PhoneNumber && channels.contains(ChannelType.SMS)
                        || address instanceof CustomerId && channels.contains(ChannelType.PUSH))
                .map(SpecificRecord.class::cast);
    }

    private static OutboundMessage buildMessage(String messageType, Stream<Pair<String, String>> params) {
        return OutboundMessage.newBuilder()
                .setTimestamp(System.currentTimeMillis())
                .setMessageType(messageType)
                .setParams(params.collect(toMap(Pair::getValue0, Pair::getValue1)))
                .build();
    }
}
