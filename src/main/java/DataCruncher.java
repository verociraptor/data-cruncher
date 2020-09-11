import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

public class DataCruncher {

    // do not modify this method - just use it to get all the Transactions that are in scope for the exercise
    public List<Transaction> readAllTransactions() throws Exception {
        return Files.readAllLines(Paths.get("src/main/resources/payments.csv"), StandardCharsets.UTF_8)
                .stream()
                .skip(1)
                .map(line -> {
                    var commaSeparatedLine = List.of(line
                            .replaceAll("'", "")
                            .split(",")
                    );
                    var ageString = commaSeparatedLine.get(2);
                    var ageInt = "U".equals(ageString) ? 0 : Integer.parseInt(ageString);
                    return new Transaction(commaSeparatedLine.get(1),
                            ageInt,
                            commaSeparatedLine.get(3),
                            commaSeparatedLine.get(4),
                            commaSeparatedLine.get(5),
                            commaSeparatedLine.get(6),
                            commaSeparatedLine.get(7),
                            Double.parseDouble(commaSeparatedLine.get(8)),
                            "1".equals(commaSeparatedLine.get(9)));
                })
                .collect(Collectors.toList());
    }

    // example
    public List<Transaction> readAllTransactionsAge0() throws Exception {
        return readAllTransactions().stream()
                .filter(transaction -> transaction.getAge() == 0)
                .collect(Collectors.toList());
    }

    // task 1

    /**
     * Returns a set of unique merchant ids from the given csv file
     * @return string set of merchant ids
     * @throws Exception
     */
    public Set<String> getUniqueMerchantIds() throws Exception {
        return readAllTransactions().stream()
                .map(Transaction::getMerchantId)
                .collect(Collectors.toSet());
    }

    // task 2

    /**
     * Returns the total number of fraudulent transactions
     * @return long, total fraudulent transactions
     * @throws Exception
     */
    public long getTotalNumberOfFraudulentTransactions() throws Exception {
        return readAllTransactions().stream()
                .filter(Transaction::isFraud).count();
    }

    // task 3

    /**
     * Return the total number of transactions given if it's fraudulent or not
     * @param isFraud
     * @return long, total number of transactions
     * @throws Exception
     */
    public long getTotalNumberOfTransactions(boolean isFraud) throws Exception {
        return readAllTransactions().stream()
                .filter(trans -> trans.isFraud() == isFraud).count();
    }

    // task 4

    /**
     * Returns a set of fraudulent transactions made by the given merchant
     * @param merchantId
     * @return set of transactions
     * @throws Exception
     */
    public Set<Transaction> getFraudulentTransactionsForMerchantId(String merchantId) throws Exception {
        return readAllTransactions().stream()
                .filter(trans -> trans.getMerchantId().equals(merchantId) && trans.isFraud())
                .collect(Collectors.toSet());
    }

    // task 5

    /**
     * Returns a set of transactions made by the given merchant and if it's fraudulent or not.
     * @param merchantId
     * @param isFraud
     * @return set of transactions
     * @throws Exception
     */
    public Set<Transaction> getTransactionsForMerchantId(String merchantId, boolean isFraud) throws Exception {
        return readAllTransactions().stream()
                .filter(trans ->
                            trans.getMerchantId().equals(merchantId) && trans.isFraud() ==isFraud)
                .collect(Collectors.toSet());
    }

    // task 6

    /**
     * Returns a list of transactions sorted by their amount
     * @return a sorted list of transactions
     * @throws Exception
     */
    public List<Transaction> getAllTransactionsSortedByAmount() throws Exception {
        return readAllTransactions().stream()
                .sorted(Comparator.comparingDouble(Transaction::getAmount))
                .collect(Collectors.toList());
    }

    // task 7

    /**
     * Returns the percentage of men that commit fraud
     * @return percentage
     * @throws Exception
     */
    public double getFraudPercentageForMen() throws Exception {
        return readAllTransactions().stream()
                .filter(trans -> trans.getGender().equals("M") && trans.isFraud()).count()
                / (double)getTotalNumberOfFraudulentTransactions();
    }

    // task 8

    /**
     * Returns a set of customer ids that commit the same amount or more fraudulent transactions
     * from the given number.
     * @param numberOfFraudulentTransactions
     * @return set of strings representing customer ids
     * @throws Exception
     */
    public Set<String> getCustomerIdsWithNumberOfFraudulentTransactions(int numberOfFraudulentTransactions) throws Exception {
        return readAllTransactions().stream()
                .filter(Transaction::isFraud)
                .collect(Collectors.collectingAndThen(
                        groupingBy(Transaction::getCustomerId, Collectors.counting()),
                        map->{
                            map.values().removeIf(num -> num < numberOfFraudulentTransactions);
                            return map.keySet();
                        }));
    }

    // task 9

    /**
     * Returns a map of customer ids to the amount of fraudulent transactions each have made.
     * @return map of strings to integers
     * @throws Exception
     */
    public Map<String, Integer> getCustomerIdToNumberOfTransactions() throws Exception {
        return readAllTransactions().stream().filter(Transaction::isFraud)
                .collect(Collectors.toMap(Transaction::getCustomerId, id -> 1, Integer::sum));
    }

    // task 10

    /**
     * Returns a map of merchants ids to the total amount that was used for their fraudulent transactions.
     * @return a map of strings to doubles
     * @throws Exception
     */
    public Map<String, Double> getMerchantIdToTotalAmountOfFraudulentTransactions() throws Exception {
        return readAllTransactions().stream().filter(Transaction::isFraud)
                .collect(Collectors.toMap(Transaction::getMerchantId, Transaction::getAmount, Double::sum));
    }

    // bonus

    /**
     * Returns the probability of the given transaction being fraudulent in the range 0 to 1.
     * It determines the probability by finding the probability of most of the variables in the
     * given transaction: customer ID, age, gender, merchant ID, category and amount (the zipcodes
     * are all the same so, for simplicity, I disregarded those).
     * It will check the probability of fraudulent transactions occurring based on each variable and
     * hence, will determine the probability of the current transaction being fraudulent.
     *
     * Note: it assumes that each of these events are independent and do not affect the other from occurring.
     * @param transaction
     * @return probability, 0 to 1
     * @throws Exception
     */
    public double getRiskOfFraudFigure(Transaction transaction) throws Exception {
        double probCustomerFraud = getProbabilityOfCustomer(transaction.getCustomerId());
        double probAgeFraud = getProbabilityOfAge(transaction.getAge());
        double probGenderFraud = getProbabilityOfGender(transaction.getGender());
        double probMerchantFraud = getProbabilityOfMerchant(transaction.getMerchantId());
        double probCatFraud = getProbabilityOfCategory(transaction.getCategory());
        double probAmntFraud = getProbabilityOfAmount(Math.round(transaction.getAmount()));

        return probCustomerFraud * probAgeFraud * probGenderFraud
                * probMerchantFraud * probCatFraud * probAmntFraud;
    }

    /**
     * Returns probability of the given customer committing a fraudulent transaction.
     * @param id, customer id
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfCustomer(String id) throws Exception {
        //amount of fraud transactions made by this customer id
        long amntFraudCustomerId = readAllTransactions().stream()
                .filter(t -> t.getCustomerId().equals(id)
                        && t.isFraud()).count();

        //amount of transactions made by this customer id
        long amntTransCustomerId = readAllTransactions().stream()
                .filter(t -> t.getCustomerId().equals(id)).count();

        return amntFraudCustomerId != 0 ? amntFraudCustomerId / (double)amntTransCustomerId : 1;
    }

    /**
     * Returns probability of a given transaction age to be fraudulent
     * @param age of transaction
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfAge(int age) throws Exception {
        //amount of fraud transactions made by this age
        long amntFraudAge = readAllTransactions().stream()
                .filter(t -> t.getAge() == age
                        && t.isFraud()).count();

        //amount of transactions made by this age
        long amntTransAge = readAllTransactions().stream()
                .filter(t -> t.getAge() == age).count();

        return amntFraudAge != 0 ? amntFraudAge/ (double)amntTransAge : 1;
    }

    /**
     * Returns probability of the given gender committing a fraudulent transaction.
     * @param gender, customer gender
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfGender(String gender) throws Exception {
        //amount of fraud transactions made by this gender
        long amntFraudGender = readAllTransactions().stream()
                .filter(t -> t.getGender().equals(gender)
                        && t.isFraud()).count();

        //amount of transactions made by this gender
        long amntTransGender = readAllTransactions().stream()
                .filter(t -> t.getGender().equals(gender)).count();

        return amntFraudGender != 0 ? amntFraudGender/ (double)amntTransGender : 1;
    }

    /**
     * Returns probability of the given merchant committing a fraudulent transaction.
     * @param id, merchant id
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfMerchant(String id) throws Exception {
        //amount of fraud transactions made by this merchant
        long amntFraudMerchantId = readAllTransactions().stream()
                .filter(t -> t.getMerchantId().equals(id)
                        && t.isFraud()).count();

        //amount of transactions made by this merchant
        long amntTransMerchantId = readAllTransactions().stream()
                .filter(t -> t.getMerchantId().equals(id)).count();

        return amntFraudMerchantId != 0 ? amntFraudMerchantId/ (double)amntTransMerchantId : 1;
    }

    /**
     * Returns probability of the given category having a fraudulent transaction.
     * @param cat, category
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfCategory(String cat) throws Exception {
        //amount of fraud transactions made by this merchant
        long amntFraudCat = readAllTransactions().stream()
                .filter(t -> t.getCategory().equals(cat)
                        && t.isFraud()).count();

        //amount of transactions made by this merchant
        long amntTransCat = readAllTransactions().stream()
                .filter(t -> t.getCategory().equals(cat)).count();

        return amntFraudCat != 0 ? amntFraudCat/ (double) amntTransCat : 1;
    }

    /**
     * Returns probability of the given amount being used for a fraudulent transaction.
     * @param amnt, amount transacted
     * @return probability, 0 to 1
     * @throws Exception
     */
    private double getProbabilityOfAmount(long amnt) throws Exception {
        //amount of fraud transactions using a close amount to this amount
        //checks in a +-5 distance of the given amount
        long amntFraudCloseToAmnt = readAllTransactions().stream()
                .filter(t -> Math.abs(Math.round(t.getAmount()) - amnt) <= 5
                        && t.isFraud()).count();

        //amount of transactions made using a close amount to this amount
        long amntTransCloseToAmnt = readAllTransactions().stream()
                .filter(t -> Math.abs(Math.round(t.getAmount()) - amnt) <= 5).count();

        return amntFraudCloseToAmnt != 0 ? amntFraudCloseToAmnt/ (double) amntTransCloseToAmnt : 1;
    }
}
