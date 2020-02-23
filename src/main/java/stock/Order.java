package stock;

import java.util.HashMap;
import java.util.Map;

;

/**
 * Author by Mao
 * kmeans data structure, and some operator
 */

class Order {
    /**
     * The user that viewed the page
     */
    public String[] orderList;
    private static final int Order_No = 0;
    private static final int Tran_Maint_Code = 1;
    private static final int Order_Price = 8;
    private static final int Order_Exec_Vol = 9;
    private static final int Order_Vol = 10;
    private static final int Sec_Code = 11;
    private static final int Trade_Dir = 22;

    // private String orderNo = new String();
    // private String tranMaintCode = new String();
    // private String orderPrice = new String();
    // private String orderExecVol = new String();
    // private String orderVol = new String();
    // private String secCode = new String();
    // private String tradeDir = new String();
    private Map<String, String> orderMap = new HashMap<String, String>();

    Order(String tuple) {
        //String[] orderList = tuple.split("\\|");
        String orderNo = new String(tuple.split("\\|")[Order_No]);
        String tranMaintCode = new String(tuple.split("\\|")[Tran_Maint_Code]);
        orderMap.put("Order_No", orderNo);
        orderMap.put("Tran_Maint_Code", tranMaintCode);
        if (!tranMaintCode.equals("")) {
            // orderPrice = new String(tuple.split("\\|")[Order_Price]);
            // orderExecVol = new String(tuple.split("\\|")[Order_Exec_Vol]);
            // orderVol = new String(tuple.split("\\|")[Order_Vol]);
            // secCode = new String(tuple.split("\\|")[Sec_Code]);
            // tradeDir = new String(tuple.split("\\|")[Trade_Dir]);
            // orderMap.put("Order_No", orderNo);
            // orderMap.put("Tran_Maint_Code", tranMaintCode);
            orderMap.put("Order_Price", new String(tuple.split("\\|")[Order_Price]));
            orderMap.put("Order_Exec_Vol", new String(tuple.split("\\|")[Order_Exec_Vol]));
            orderMap.put("Order_Vol", new String(tuple.split("\\|")[Order_Vol]));
            orderMap.put("Sec_Code", new String(tuple.split("\\|")[Sec_Code]));
            orderMap.put("Trade_Dir", new String(tuple.split("\\|")[Trade_Dir]));
        }
    }

    String getOrderNo() {
        return orderMap.get("Order_No");
    }
    String getTranMaintCode() {
        return orderMap.get("Tran_Maint_Code");
    }
    float getOrderPrice() {
        return Float.parseFloat(orderMap.get("Order_Price"));
    }
    int getOrderExecVol() {
        Float interOrderExecVol = Float.parseFloat(orderMap.get("Order_Exec_Vol"));
        return interOrderExecVol.intValue();
    }
    int getOrderVol() {
        Float interOrderVol = Float.parseFloat(orderMap.get("Order_Vol"));
        return interOrderVol.intValue();
    }
    String getSecCode() {
        return orderMap.get("Sec_Code");
    }
    String getTradeDir() {
        return orderMap.get("Trade_Dir");
    }

    String getKey(String key) {
        return orderMap.get(key);
    }

    String objToString() {
        StringBuilder messageBuilder = new StringBuilder();
        messageBuilder.append(orderMap.get("Order_No")).append("|");
        messageBuilder.append(orderMap.get("Tran_Maint_Code")).append("|");
        messageBuilder.append(orderMap.get("Order_Price")).append("|");
        messageBuilder.append(orderMap.get("Order_Exec_Vol")).append("|");
        messageBuilder.append(orderMap.get("Order_Vol")).append("|");
        messageBuilder.append(orderMap.get("Sec_Code")).append("|");
        messageBuilder.append(orderMap.get("Trade_Dir"));
        return messageBuilder.toString();
        // return String.join("|", this.orderList);
    }

    public boolean updateOrder(int otherOrderVol) {
        orderMap.put("Order_Vol", (this.getOrderVol() - otherOrderVol) + "");
        orderMap.put("Order_Exec_Vol", (this.getOrderExecVol() + otherOrderVol) + "");
        return true;
    }
}
