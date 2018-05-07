package drpc;

import org.apache.storm.thrift.TException;
import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

public class RemoteDRPCClient {

    public static void main(String[] args) throws TException {

        DRPCClient drpcClient = new DRPCClient(Utils.readStormConfig(), "127.0.0.1", 3227);

        for (Integer num : new Integer[]{12, 22, 21})
            System.out.println("Result for " + num + " = " + drpcClient.execute("remote-drpc", num.toString()));
    }
}
