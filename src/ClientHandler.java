import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * ClientHandler: handles a single client connection's lifecycle.
 *
 * OS concept: Concurrency model
 * ------------------------------
 * The Server accepts a connection and hands it off to a ClientHandler,
 * which runs on its own thread (see Server -> ExecutorService). This is
 * the "thread-per-connection" model. It's simple to reason about, but
 * has a real trade-off worth knowing for interviews: each thread
 * consumes OS resources (stack memory, context-switch overhead), so
 * this model doesn't scale to tens of thousands of connections the way
 * an event-loop (like Node.js or Redis's own single-threaded loop)
 * would. We use a bounded thread pool (ExecutorService) specifically to
 * cap that cost rather than spawning unbounded raw Threads.
 */
public class ClientHandler implements Runnable {

    private final Socket clientSocket;
    private final CommandProcessor commandProcessor;

    public ClientHandler(Socket clientSocket, CommandProcessor commandProcessor) {
        this.clientSocket = clientSocket;
        this.commandProcessor = commandProcessor;
    }

    @Override
    public void run() {
        String clientId = clientSocket.getRemoteSocketAddress().toString();
        System.out.println("[+] Client connected: " + clientId);

        try (
            BufferedReader in = new BufferedReader(
                new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
            OutputStream out = clientSocket.getOutputStream()
        ) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.equalsIgnoreCase("QUIT") || line.equalsIgnoreCase("EXIT")) {
                    break;
                }
                String response = commandProcessor.process(line);
                out.write((response + "\r\n").getBytes(StandardCharsets.UTF_8));
                out.flush();
            }
        } catch (IOException e) {
            System.out.println("[!] Connection error with " + clientId + ": " + e.getMessage());
        } finally {
            try {
                clientSocket.close();
            } catch (IOException ignored) {
            }
            System.out.println("[-] Client disconnected: " + clientId);
        }
    }
}
