/********************************************************************************
 * Copyright (c) 2014-2022 Cirrus Link Solutions and others
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0.
 *
 * SPDX-License-Identifier: EPL-2.0
 *
 * Contributors:
 *   Cirrus Link Solutions - initial implementation
 ********************************************************************************/

package org.eclipse.tahu.message.model;

import org.eclipse.tahu.SparkplugParsingException;

/**
 * An enumeration of Sparkplug MQTT message types. The type provides an indication as to what the MQTT Payload of
 * message will contain.
 */
public enum MessageType {

	/**
	 * Birth certificate for MQTT Edge of Network (EoN) Nodes.
	 */
	NBIRTH,

	/**
	 * Death certificate for MQTT Edge of Network (EoN) Nodes.
	 */
	NDEATH,

	/**
	 * Birth certificate for MQTT Devices.
	 */
	DBIRTH,

	/**
	 * Death certificate for MQTT Devices.
	 */
	DDEATH,

	/**
	 * Edge of Network (EoN) Node data message.
	 */
	NDATA,

	/**
	 * Device data message.
	 */
	DDATA,

	/**
	 * Edge of Network (EoN) Node command message.
	 */
	NCMD,

	/**
	 * Device command message.
	 */
	DCMD,

	/**
	 * Critical application state message.
	 */
	STATE,

	/**
	 * Device record message.
	 */
	DRECORD,

	/**
	 * Edge of Network (EoN) Node record message.
	 */
	NRECORD;

	/**
	 * Parses a Sparkplug message type which MUST be one of the valid {@link MessageType}s
	 *
	 * @param type the {@link String} representing the potential {@link MessageType}
	 *
	 * @return the {@link MessageType} that represents the {@link String} type argument
	 *
	 * @throws SparkplugParsingException if the incoming {@link String} type does not represent a {@link MessageType}
	 */
	public static MessageType parseMessageType(String type) throws SparkplugParsingException {
		for (MessageType messageType : MessageType.values()) {
			if (messageType.name().equals(type)) {
				return messageType;
			}
		}
		throw new SparkplugParsingException("Invalid message type: " + type);
	}

	/**
	 * Whether or not this is an NDEATH or DDEATH
	 *
	 * @return true if this {@link MessageType} is an NDEATH or DDEATH
	 */
	public boolean isDeath() {
		return this.equals(DDEATH) || this.equals(NDEATH);
	}

	/**
	 * Whether or not this is an NCMD or DCMD
	 *
	 * @return true if this {@link MessageType} is an NCMD or DCMD
	 */
	public boolean isCommand() {
		return this.equals(DCMD) || this.equals(NCMD);
	}

	/**
	 * Whether or not this is an NDATA or DDATA
	 *
	 * @return true if this {@link MessageType} is an NDATA or DDATA
	 */
	public boolean isData() {
		return this.equals(DDATA) || this.equals(NDATA);
	}

	/**
	 * Whether or not this is an NBIRTH or DBIRTH
	 *
	 * @return true if this {@link MessageType} is an NBIRTH or DBIRTH
	 */
	public boolean isBirth() {
		return this.equals(DBIRTH) || this.equals(NBIRTH);
	}

	/**
	 * Whether or not this is an NRECORD or DRECORD
	 *
	 * @return true if this {@link MessageType} is an NRECORD or DRECORD
	 */
	public boolean isRecord() {
		return this.equals(DRECORD) || this.equals(NRECORD);
	}

	/**
	 * Whether or not this is an Edge Node level Message Type (NBIRTH, NDATA, NCMD, NDEATH, or NRECORD)
	 *
	 * @return true if this is an Edge Node level Message Type, otherwise false
	 */
	public boolean isEdgeNodeMessage() {
		return this.equals(NBIRTH) || this.equals(NDATA) || this.equals(NCMD) || this.equals(NDEATH)
				|| this.equals(NRECORD);
	}

	/**
	 * Whether or not this is an Device level Message Type (DBIRTH, DDATA, DCMD, DDEATH, or DRECORD)
	 *
	 * @return true if this is an Device level Message Type, otherwise false
	 */
	public boolean isDeviceMessage() {
		return this.equals(DBIRTH) || this.equals(DDATA) || this.equals(DCMD) || this.equals(DDEATH)
				|| this.equals(DRECORD);
	}
}
