import java.util.Objects;

public class Transaction {
    private final String customerId;
    private final int age;
    private final String gender;
    private final String customerZipCode;
    private final String merchantId;
    private final String merchantZipCode;
    private final String category;
    private final double amount;
    private final boolean isFraud;

    public Transaction(String customerId, int age, String gender, String customerZipCode, String merchantId, String merchantZipCode, String category, double amount, boolean isFraud) {
        this.customerId = customerId;
        this.age = age;
        this.gender = gender;
        this.customerZipCode = customerZipCode;
        this.merchantId = merchantId;
        this.merchantZipCode = merchantZipCode;
        this.category = category;
        this.amount = amount;
        this.isFraud = isFraud;
    }

    public String getCustomerId() {
        return customerId;
    }

    public int getAge() {
        return age;
    }

    public String getGender() {
        return gender;
    }

    public String getCustomerZipCode() {
        return customerZipCode;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public String getMerchantZipCode() {
        return merchantZipCode;
    }

    public String getCategory() {
        return category;
    }

    public double getAmount() {
        return amount;
    }

    public boolean isFraud() {
        return isFraud;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Transaction that = (Transaction) o;
        return age == that.age &&
                Double.compare(that.amount, amount) == 0 &&
                isFraud == that.isFraud &&
                Objects.equals(customerId, that.customerId) &&
                Objects.equals(gender, that.gender) &&
                Objects.equals(customerZipCode, that.customerZipCode) &&
                Objects.equals(merchantId, that.merchantId) &&
                Objects.equals(merchantZipCode, that.merchantZipCode) &&
                Objects.equals(category, that.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(customerId, age, gender, customerZipCode, merchantId, merchantZipCode, category, amount, isFraud);
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "customerId='" + customerId + '\'' +
                ", age=" + age +
                ", gender='" + gender + '\'' +
                ", customerZipCode='" + customerZipCode + '\'' +
                ", merchantId='" + merchantId + '\'' +
                ", merchantZipCode='" + merchantZipCode + '\'' +
                ", category='" + category + '\'' +
                ", amount=" + amount +
                ", isFraud=" + isFraud +
                '}';
    }
}
