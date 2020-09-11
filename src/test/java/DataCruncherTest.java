import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class DataCruncherTest {
    private final DataCruncher dataCruncher = new DataCruncher();

    // ignore
    @Test
    public void readAllTransactions() throws Exception {
        var transactions = dataCruncher.readAllTransactions();
        assertEquals(594643, transactions.size());
    }

    // example
    @Test
    public void readAllTransactionsAge0() throws Exception {
        var transactions = dataCruncher.readAllTransactionsAge0();
        assertEquals(3630, transactions.size());
    }

    // task1
    @Test
    public void getUniqueMerchantIds() throws Exception {
        var transactions = dataCruncher.getUniqueMerchantIds();
        assertEquals(50, transactions.size());
    }

    // task2
    @Test
    public void getTotalNumberOfFraudulentTransactions() throws Exception {
        var totalNumberOfFraudulentTransactions = dataCruncher.getTotalNumberOfFraudulentTransactions();
        assertEquals(297508, totalNumberOfFraudulentTransactions);
    }

    // task3
    @Test
    public void getTotalNumberOfTransactions() throws Exception {
        assertEquals(297508, dataCruncher.getTotalNumberOfTransactions(true));
        assertEquals(297135, dataCruncher.getTotalNumberOfTransactions(false));
    }

    // task4
    @Test
    public void getFraudulentTransactionsForMerchantId() throws Exception {
        Set<Transaction> fraudulentTransactionsForMerchantId = dataCruncher.getFraudulentTransactionsForMerchantId("M1823072687");
        assertEquals(149001, fraudulentTransactionsForMerchantId.size());
    }

    // task5
    @Test
    public void getTransactionForMerchantId() throws Exception {
        assertEquals(102588, dataCruncher.getTransactionsForMerchantId("M348934600", true).size());
        assertEquals(102140, dataCruncher.getTransactionsForMerchantId("M348934600", false).size());
    }

    // task6
    @Test
    public void getAllTransactionSortedByAmount() throws Exception {
        List<Transaction> allTransactionsSortedByAmount = dataCruncher.getAllTransactionsSortedByAmount();
        boolean isSorted = true;
        for(int i = 0; i < allTransactionsSortedByAmount.size() - 1; i++){
            if(allTransactionsSortedByAmount.get(i).getAmount()
                    > allTransactionsSortedByAmount.get(i + 1).getAmount()) {
                isSorted = false;
                break;
            }
        }
        assertTrue(isSorted);
    }

    // task7
    @Test
    public void getFraudPercentageForMen() throws Exception {
        double fraudPercentageForMen = dataCruncher.getFraudPercentageForMen();
        assertEquals(0.45, fraudPercentageForMen, 0.01);
    }

    // task8
    // tested also using dummy data from payment - Copy.csv
    // tested by getting the correct data from pivot tables in excel
    @Test
    public void getCustomerIdsWithNumberOfFraudulentTransactions() throws Exception {
        Set<String> customerIdsWithNumberOfFraudulentTransactions = dataCruncher.getCustomerIdsWithNumberOfFraudulentTransactions(3);
        boolean customersBelong = true;
        String csvFile = "src/main/resources/sets-customers-frauds-three.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

       br = new BufferedReader(new FileReader(csvFile));
       while ((line = br.readLine()) != null) {
           String[] custTrans = line.replaceAll("'", "").split(cvsSplitBy);

           if(!customerIdsWithNumberOfFraudulentTransactions.contains(custTrans[0])){
               customersBelong = false;
               break;
           }


       }
       assertEquals(4110, customerIdsWithNumberOfFraudulentTransactions.size());
       assertTrue(customersBelong);
    }

    // task9
    //tested also using dummy data from payment - Copy.csv
    // tested by getting the correct data from pivot tables in excel
    @Test
    public void getCustomerIdToNumberOfTransactions() throws Exception {
        Map<String, Integer> customerIdToNumberOfTransactions = dataCruncher.getCustomerIdToNumberOfTransactions();
        boolean customersBelong = true;
        String csvFile = "src/main/resources/maps-customers-frauds.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        br = new BufferedReader(new FileReader(csvFile));
        while ((line = br.readLine()) != null) {
            String[] custTrans = line.replaceAll("'", "").split(cvsSplitBy);

            if(!customerIdToNumberOfTransactions.containsKey(custTrans[0]) ||
                    customerIdToNumberOfTransactions.get(custTrans[0]) != Integer.parseInt(custTrans[1])){
                customersBelong = false;
                break;
            }


        }
        assertEquals(4112, customerIdToNumberOfTransactions.size());
        assertTrue(customersBelong);
    }

    // task10
    //tested also using dummy data from payment - Copy.csv
    // tested by getting the correct data from pivot tables in excel
    @Test
    public void getMerchantIdToTotalAmountOfFraudulentTransactions() throws Exception {
        Map<String, Double> merchantIdToTotalAmountOfFraudulentTransactions = dataCruncher.getMerchantIdToTotalAmountOfFraudulentTransactions();
        boolean merchantsBelong = true;
        String csvFile = "src/main/resources/maps-merchants-frauds.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        br = new BufferedReader(new FileReader(csvFile));
        while ((line = br.readLine()) != null) {
            String[] custTrans = line.replaceAll("'", "").split(cvsSplitBy);

            double currMapAmnt = Math.round(merchantIdToTotalAmountOfFraudulentTransactions.get(custTrans[0]) * 100.0)/100.0;
            double csvAmnt = Double.parseDouble(custTrans[1]);
            if(!merchantIdToTotalAmountOfFraudulentTransactions.containsKey(custTrans[0]) ||
                    currMapAmnt != csvAmnt){
                merchantsBelong = false;
                break;
            }


        }
        assertEquals(49, merchantIdToTotalAmountOfFraudulentTransactions.size());
        assertTrue(merchantsBelong);
    }

    //bonus
    @Test
    public void getRiskOfFraudFigure() throws Exception {
        Transaction t = new Transaction("C1499363341",5,	"M",	"28007",
                "M1823072687",	"28007",	"es_transportation", 14.46,true);
        System.out.println(dataCruncher.getRiskOfFraudFigure(t));

    }
}