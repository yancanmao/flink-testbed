package stock;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class OrderPool {
    public HashMap<Integer, ArrayList<Order>> buyPool = new HashMap<>();
    public HashMap<Integer, ArrayList<Order>> sellPool = new HashMap<>();

    OrderPool(HashMap<Integer, ArrayList<Order>> curBuyPool, HashMap<Integer, ArrayList<Order>> curSellPool) {
        this.buyPool = curBuyPool;
        this.sellPool = curSellPool;
    }
}
