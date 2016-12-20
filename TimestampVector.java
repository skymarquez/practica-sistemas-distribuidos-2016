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

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Arrays;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}
	
	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public synchronized void updateTimestamp(Timestamp timestamp){
		
	   /**
		* El método updateTimestamp debe actualizar el vector poniendo el timestamp que se pasa por parámetro
		* en la posición correspondiente (la del hostid que corresponda).
		*/
		if (timestamp != null) {
			this.timestampVector.put(timestamp.getHostid(), timestamp);
	    }
			
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public synchronized void updateMax(TimestampVector tsVector){
//        StringBuilder sb = new StringBuilder("TimestampVector - UpdateMax... This vector: ");
//        sb.append(this);
//        sb.append(" - tsVector vector: ");
//        sb.append(tsVector);

        /**
         * Recorre todos los nodos del vector actual y busca el mismo nodo en el otro vector. 
         * Si existe uno igual y su timestamp es mayor que la actual, 
         * la marca de tiempo actual se reemplaza.
         * 
         */		
        for (String node : this.timestampVector.keySet()) {
            Timestamp otherTimestamp = tsVector.getLast(node);

            if (otherTimestamp == null) {
                continue;
            } else if (this.getLast(node).compare(otherTimestamp) < 0) {
                this.timestampVector.replace(node, otherTimestamp);
            }
        }
//        sb.append("Updated This Vector: ");
//        sb.append(this);
//        System.out.println(sb);		
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public synchronized Timestamp getLast(String node){	
		 return this.timestampVector.get(node);
	}
		// return generated automatically. Remove it when implementing your solution 
	
	
	/**
	 * Fusiona el vector timestamp local con el vector timestamp tsVector 
	 * tomando el timestamp más pequeño para cada nodo. Después de la fusión, 
	 * el nodo local tendrá la marca de tiempo más pequeña para cada nodo.
	 * 
	 *  @param tsVector (timestamp vector)
	 */
	public synchronized void mergeMin(TimestampVector tsVector){
		
		 if (tsVector == null) {
	            return;
	        }
//	        StringBuilder sb = new StringBuilder("TimestampVector - MergeMin... This vector: ");
//	        sb.append(this);
//	        sb.append(" - tsVector vector: ");
//	        sb.append(tsVector);

	        /**
	         * Similar a updateMax con la diferencia de que también agrega nuevas entradas 
	         * si aún no existen en el vector de timestamp actual. Esto se debe a la forma en 
	         * que esta función es utilizada por TimetampMatrix.
	         * 
	         */
	        for (Map.Entry<String, Timestamp> entry : tsVector.timestampVector.entrySet()) {
	            String node = entry.getKey();
	            Timestamp otherTimestamp = entry.getValue();
	            Timestamp thisTimestamp = this.getLast(node);
	            
	            if (thisTimestamp == null) {
	                this.timestampVector.put(node, otherTimestamp);
	            } else if (otherTimestamp.compare(thisTimestamp) < 0) {
	                this.timestampVector.replace(node, otherTimestamp);
	            }
	        }
//	        sb.append("Updated This Vector: ");
//	        sb.append(this);
//	        System.out.println(sb);
	
	}

	
	/**
	 * clone
	 */
//	@Override
	public  TimestampVector clone(){
			
		/**
		* 1. Crear la tabla con los participantes
		* Crear tabla utilizando el constructor con la medida del nº de participantes. No vector.		
		* Crear el nuevo TimestampVector con los participantes
		*/
		String[]participants =timestampVector.keySet().toArray(new String[timestampVector.keySet().size()]);
		TimestampVector timeclone = new TimestampVector(Arrays.asList(participants));   

		for (String node : this.timestampVector.keySet()) {
	            Timestamp thisTimestamp = this.getLast(node);
//	            System.out.println("Timestamp antes update: " + timeclone.getLast(node));
	            timeclone.updateTimestamp(thisTimestamp);
//	            System.out.println("Timestamp despues update: '" + timeclone.getLast(node) + "' debe ser igual que '" + timestampVector.get(node) + "'");            
	    }   
//		System.out.println("Equal0: " + timeclone.toString()==timestampVector.toString());
//		System.out.println("Equal1: " + timestampVector.equals(timeclone));
//      System.out.println("Equal2: " + timeclone.equals(timestampVector));
      		
		return timeclone; 
		
	}
		// return generated automatically. Remove it when implementing your solution 


	public boolean equals(TimestampVector tsVector){

//		System.out.print("Inicio equals timestamp");
	        if (tsVector == null) {
	            return false;
	        } else if (!(tsVector instanceof TimestampVector)) {
	            return false;
	        }
//	        System.out.print(". No es null y es Instancia de TimestampVector");
	        
	        TimestampVector other = (TimestampVector) tsVector;
	        
	        if (!other.timestampVector.equals(this.timestampVector)) {
//	        	System.out.println("Equal interno no es igual");
	        	return false;                 
	        }  	        
	        return this.timestampVector.equals(other.timestampVector);	        
	    }

		// return generated automatically. Remove it when implementing your solution        

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}
