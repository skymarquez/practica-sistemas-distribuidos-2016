/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.sessions;

import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.TimerTask;
import java.util.Vector;
//import java.util.ArrayList;

import recipes_service.ServerData;
import recipes_service.activity_simulation.SimulationData;
import recipes_service.communication.Host;
import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;



import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.RemoveOperation;


/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionOriginatorSide extends TimerTask{
	private ServerData serverData;
	
	public TSAESessionOriginatorSide(ServerData serverData){
		super();
		this.serverData=serverData;		
	}
	
	/**
	 * Implementation of the TimeStamped Anti-Entropy protocol
	 */
	public void run(){
		sessionWithN(serverData.getNumberSessions());
	}

	/**
	 * This method performs num TSAE sessions
	 * with num random servers
	 * @param num
	 */
	public void sessionWithN(int num){
		if(!SimulationData.getInstance().isConnected())
			return;
		List<Host> partnersTSAEsession= serverData.getRandomPartners(num);
		Host n;
		for(int i=0; i<partnersTSAEsession.size(); i++){
			n=partnersTSAEsession.get(i);
			sessionTSAE(n);
		}
	}
	
	/**
	 * This method perform a TSAE session
	 * with the partner server n
	 * @param n
	 */

	private void sessionTSAE(Host n){
		if (n == null) return;

//		System.out.println("Originator starts TSAE session with ... " + n);

		 
		try {
			Socket socket = new Socket(n.getAddress(), n.getPort());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());

//			System.out.println("Originator - opened streams");
			
			// get a copy of the actual ACK and Summary
			TimestampMatrix localAck;
			TimestampVector localSummary;	

	        /**
	         * Recopila una instantánea de localSummary y localAck. 
	         * Se sincroniza para que la matriz o el resumen no cambie entre ellos.
	         * También este es el único lugar donde el vector local en la matriz 
	         * se actualiza con el localSummary.
             * 
             */
			//Synchronized them
            synchronized (serverData) {
  //           System.out.println("Originator - begin to clone summary");
                localSummary = this.serverData.getSummary().clone();
//             System.out.println("Originator - finish to clone summary");
//             System.out.println(localSummary.toString());               
//				serverData.getAck().update(serverData.getId(), localSummary);               
//             System.out.println("Originator - begin to clone ack");
                localAck = this.serverData.getAck().clone();
//             System.out.println("Originator - finish to clone ack");
//             System.out.println(localAck.toString());
            }          
//             System.out.println("Originator - collected local Summary and Ack");
                     
            /**
             * Envía el resumen y la matriz ack para el otro lado
             *
             */
            // send localSummary and localAck

			Message	msg = new MessageAErequest(localSummary, localAck);  
			out.writeObject(msg);
			
//			System.out.println("Originator - sent AE Request");
			
// 			receive operations from partner

			List<MessageOperation> operations = new Vector<MessageOperation>();
			
			msg = (Message) in.readObject();
			
            /**
             * Recopila todas las operaciones que tiene la el otro lado 
             * pero el servidor actual no. 
             * La ejecución sucede después de que sepamos que TSAE tuvo éxito.
             * 
             */
			while (msg.type() == MsgType.OPERATION){

//				System.out.println("Originator - received operation: ");
				
				operations.add((MessageOperation) msg);			
//				System.out.println("Originator - remembered operation");				
				msg = (Message) in.readObject();
			}
			
			if (msg.type() == MsgType.AE_REQUEST){
													
			MessageAErequest aeMsg = (MessageAErequest) msg;
			
//			System.out.println("Originator - received AE Request");
			
            /**
             * Obtiene todas las operaciones más recientes que las 
             * que tiene el otro lado (el resumen recibido) y las envía al otro lado.
             * 
             */
			for (Operation op : serverData.getLog().listNewer(aeMsg.getSummary())) {
                out.writeObject(new MessageOperation(op));
            }
            
//			System.out.println("Originator - sent operations");
			
			// send operations
			// send and "end of TSAE session" message
			msg = new MessageEndTSAE();  
			out.writeObject(msg);			
//			System.out.println("Originator - sent EndTSAE");	
			// receive message to inform about the ending of the TSAE session
			msg = (Message) in.readObject();
			
			if (msg.type() == MsgType.END_TSAE){

//			System.out.println("Originator - received EndTSAE");				
//			if its ok do all operations
			
                /**
                 * TSEA tuvo éxito => ejecuta las operaciones recopiladas y actualizar los datos del servidor local.
                 * 
                 */
                synchronized (serverData) {
                    for (MessageOperation op : operations) {
                        if (op.getOperation().getType() == OperationType.ADD) {
                            serverData.execOperation((AddOperation) op.getOperation());
                        } else {
                            serverData.execOperation((RemoveOperation) op.getOperation());
                        }
                    }                  
//          System.out.println("Originator - implemented all operations");                  
//			updated summary and ACK      
                     serverData.getSummary().updateMax(aeMsg.getSummary());
                     serverData.getAck().updateMax(aeMsg.getAck());
                     serverData.getLog().purgeLog(serverData.getAck());
                     
//          System.out.println("Originator - updated Summary and Ack");
                  
				 } 
			}
		}				
			socket.close();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
            System.exit(1);
		} catch (IOException e) {
	    }	
//		System.out.println("...originator finished TSAE session with " + n);
	}
     
}
