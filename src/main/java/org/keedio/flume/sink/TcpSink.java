/**
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.keedio.flume.sink;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.io.IOException;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TcpSink extends AbstractSink implements Configurable{

	private static final Logger logger = LoggerFactory.getLogger(TcpSink.class);
	private Socket socket = null;
	private String serverAddress;
	private Integer serverPort;
	private Integer connectionRetries;
	private Integer connectionTimeout;
	private Integer connectionRetryDelay;
	private Integer currentRetries;
	private SinkCounter sinkCounter;
	private Integer batchSize;
	private static final byte[] returnCarriage = System.getProperty("line.separator").getBytes();

	@Override
	public void configure(Context context) {

		serverAddress = context.getString("hostname");
		serverPort = context.getInteger("port");

		Preconditions.checkState(serverAddress != null, "No serverAdress specified");
		Preconditions.checkState(serverPort != null, "No serverPort specified");

		connectionRetries = context.getInteger("connectionRetries",0);
		connectionTimeout = context.getInteger("connectionTimeout",10);
		connectionRetryDelay = context.getInteger("connectionRetryDelay",10);
		batchSize = context.getInteger("batchSize",100);
		currentRetries = 0;

		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}

	}

	@Override
	public void start() {
		logger.info("Starting {}...", this);
		sinkCounter.start();

		destroyConnection();
		createConnection();
		super.start();
	}


	@Override
	public void stop () {
		try {
			socket.close();
			sinkCounter.incrementConnectionClosedCount();
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
		} finally {
			super.stop();
		}
	}

	@Override
	public Status process() throws EventDeliveryException {

		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction txn = channel.getTransaction();

		try {
			txn.begin();
			
			verifyConnection();
			
			byte[] batch = new byte[0];
			int batchToSendSize = 0;
			
			for (int i = 0; i < batchSize; i++) {
				Event event = channel.take();
				
				if (event != null){
					byte[] body = event.getBody();
					if (body!=null){
						byte[] newBatch = new byte[batch.length + body.length + returnCarriage.length];
						System.arraycopy(batch, 0, newBatch, 0, batch.length);
						System.arraycopy(body, 0, newBatch, batch.length, body.length);
						System.arraycopy(returnCarriage, 0, newBatch, batch.length + body.length, returnCarriage.length);
						batch = newBatch;
						batchToSendSize++;
					}
				}else{
					break;
				}
			}
			
			if (batchToSendSize == 0){
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			}else{
				if (batchToSendSize < batchSize){
					sinkCounter.incrementBatchUnderflowCount();
				}else{
					sinkCounter.incrementBatchCompleteCount();
				}
				
				sinkCounter.addToEventDrainAttemptCount(batchToSendSize);
				socket.getOutputStream().write(batch);	
			}

			txn.commit();
			sinkCounter.addToEventDrainSuccessCount(batchToSendSize);
			
		} catch (Throwable t){
			if (txn != null) {
				txn.rollback();
			}
			if (t instanceof IOException){			
				logger.error(t.getMessage(),t);
				destroyConnection();
			}
			else if (t instanceof ChannelException){
				logger.error("TCP Sink " + getName() + ": Unable to get event from" +
		                " channel " + channel.getName() + ". Exception follows.", t);
		        status = Status.BACKOFF;
			}
		} finally {
			txn.close();
	    }

		return status;
	}
	
	private void verifyConnection() {
		if (socket == null){
			createConnection();
		} else if (socket.isClosed() || !socket.isConnected()){
			destroyConnection();
			createConnection();
		}
		
	}

	private void createConnection() {

		try{
			if (socket == null){
				logger.info("Connecting to {}:{}", serverAddress, serverPort);
				socket = new Socket();
				socket.connect(new InetSocketAddress(serverAddress, serverPort), connectionTimeout);

				logger.info("Succesfully Connected to {}:{}", serverAddress, serverPort);

				connectionRetries=0;
				sinkCounter.incrementConnectionCreatedCount();
			}
		} catch (UnknownHostException e) {
			logger.error("Unknown Host " + serverAddress + ". Unable to create Tcp client sink",e);
			sinkCounter.incrementConnectionFailedCount();
			throw new FlumeException(e);
		} catch (IOException e) {
			logger.error(e.getMessage(),e);
			sinkCounter.incrementConnectionFailedCount();
			try {
				if (connectionRetries == 0 || currentRetries < connectionRetries){
					logger.info("Sleeping {} seconds before try to reconnect", connectionRetryDelay);
					Thread.sleep(connectionRetryDelay*1000);					
					
					logger.info("Retrying connection to {}:{}", serverAddress, serverPort);
					connectionRetries++;
					verifyConnection();
				}
				else {
					logger.error("Max connection retries performed. Stopping sink");
					throw new FlumeException(e);
				}
			} catch (InterruptedException e1) {
				logger.error(e.getMessage(),e);
			}
		}
	}
	

	private void destroyConnection() {
		if (socket != null){
			logger.info("Closing socket connection {}:{}", serverAddress, serverPort);
			if (!socket.isClosed()){
				try{
					socket.close();
					sinkCounter.incrementConnectionClosedCount();
				} catch (IOException e){
					logger.error(e.getMessage(),e);
				}
			}
			socket = null;
		}
	}

}