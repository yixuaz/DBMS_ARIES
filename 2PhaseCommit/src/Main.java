import client.Client;
import server.Participant;
import server.TxnCoordinator;

public class Main {
    /* should output
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0
    0, 1, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 0
    0, 1, 2, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 1, 0
    0, 1, 2, 3, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 3, 2, 1, 0
    0, 1, 2, 3, 4, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 3, 2, 1, 0
    0, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 6, 6, 6, 5, 4, 3, 2, 1, 0
    0, 1, 2, 3, 4, 5, 6, 7, 7, 7, 7, 7, 7, 6, 5, 4, 3, 2, 1, 0
    0, 1, 2, 3, 4, 5, 6, 7, 8, 8, 8, 8, 7, 6, 5, 4, 3, 2, 1, 0
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0
     */
    public static void main(String[] args) throws InterruptedException {
        TxnCoordinator txnCoordinator = new TxnCoordinator(100, 20, 0);
        Client client = new Client(txnCoordinator, 200);
        for (int i = 1; i < 10; i++) {
            System.out.println(client.queryAllShardDbPages());
            client.updateInTxn(i, 19 - i);
        }
        System.out.println(client.queryAllShardDbPages());
        client.getTxnCoordinator().shutDown();
        for (Participant p : client.getTxnCoordinator().getParticipants()) p.shutDown();
    }
}
