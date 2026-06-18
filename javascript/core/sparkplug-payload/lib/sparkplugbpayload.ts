/********************************************************************************
 * Copyright (c) 2016-2018 Cirrus Link Solutions and others
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

import * as ProtoRoot from './sparkplugPayloadProto';
import Long from 'long';
import type * as IProtoRoot from './sparkplugPayloadProto';
import type { Reader } from 'protobufjs';

const Payload = ProtoRoot.org.eclipse.tahu.protobuf.Payload;
const Template = Payload.Template;
const Parameter = Template.Parameter;
const DataSet = Payload.DataSet;
const DataSetValue = DataSet.DataSetValue;
const Row = DataSet.Row;
const PropertyValue = Payload.PropertyValue;
const PropertySet = Payload.PropertySet;
const PropertySetList = Payload.PropertySetList;
const MetaData = Payload.MetaData;
const Metric = Payload.Metric;

// import generated interfaces
type IPayload = IProtoRoot.org.eclipse.tahu.protobuf.Payload.$Properties;
type ITemplate = IProtoRoot.org.eclipse.tahu.protobuf.Payload.Template.$Properties;
type IParameter = IProtoRoot.org.eclipse.tahu.protobuf.Payload.Template.Parameter.$Properties;
type IDataSet = IProtoRoot.org.eclipse.tahu.protobuf.Payload.DataSet.$Properties;
type IDataSetValue = IProtoRoot.org.eclipse.tahu.protobuf.Payload.DataSet.DataSetValue.$Properties;
type IRow = IProtoRoot.org.eclipse.tahu.protobuf.Payload.DataSet.Row.$Properties;
type IPropertyValue = IProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertyValue.$Properties;
type IPropertySet = IProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertySet.$Properties;
type IPropertySetList = IProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertySetList.$Properties;
type IMetaData = IProtoRoot.org.eclipse.tahu.protobuf.Payload.MetaData.$Properties;
type IMetric = IProtoRoot.org.eclipse.tahu.protobuf.Payload.Metric.$Properties;

// "user types"
export type TypeStr = "Int8"
    | "Int16"
    | "Int32"
    | "Int64"
    | "UInt8"
    | "UInt16"
    | "UInt32"
    | "UInt64"
    | "Float"
    | "Double"
    | "Boolean"
    | "String"
    | "DateTime"
    | "Text"
    | "UUID"
    | "DataSet"
    | "Bytes"
    | "File"
    | "Template"
    | "PropertySet"
    | "PropertySetList"
    | "Int8Array"
    | "Int16Array"
    | "Int32Array"
    | "Int64Array"
    | "UInt8Array"
    | "UInt16Array"
    | "UInt32Array"
    | "UInt64Array"
    | "FloatArray"
    | "DoubleArray"
    | "BooleanArray"
    | "StringArray";

export interface UMetric extends Omit<IMetric, 'value' | 'properties'> {
    value: null | number | Long.Long | boolean | string | Uint8Array | UDataSet | UTemplate | boolean[] | string[] | number[];
    type: TypeStr;
    properties?: Record<string, UPropertyValue>
}
export interface UPropertyValue extends Omit<IPropertyValue, 'type' | 'value'> { // TODO is the type supposed to be like the metric type in the readme?
    value: null | number | Long.Long | boolean | string | UPropertySet | UPropertySetList;
    type: TypeStr;
}
export interface UParameter extends Omit<IParameter, 'type' | 'value'> { // TODO is the type supposed to be like the metric type in the readme?
    value: number | Long.Long | boolean | string | UPropertySet | UPropertySetList;
    type: TypeStr;
}
export interface UTemplate extends Omit<ITemplate, 'metrics' | 'parameters'> { // TODO is the type supposed to be like the metric type in the readme?
    metrics?: UMetric[];
    parameters?: UParameter[];
}
export interface UDataSet extends Omit<IDataSet, 'types' | 'rows'> {
    types: TypeStr[];
    rows: UDataSetValue[][];
}
export type UDataSetValue = number | Long.Long | boolean | string;
export type UPropertySet = Record<string, UPropertyValue>;
export type UPropertySetList = UPropertySet[];
export type UserValue = UMetric['value'] | UPropertyValue['value'] | UDataSet | UDataSetValue | UPropertySet | UPropertySetList;
export interface UPayload extends Omit<IPayload, 'metrics'> {
    metrics?: UMetric[] | null;
}

/**
 * Sets the value of an object given it's type expressed as an integer
 * 
 * only used during encode functions
 */
function setValue (type: number, value: UserValue, object: IMetric | IPropertyValue) {
    // TODO not sure about type casts
    switch (type) {
        case 1: // Int8
        case 2: // Int16
        case 3: // Int32
        case 5: // UInt8
        case 6: // UInt16
            object.intValue = value as number;
            break;
        case 4: // Int64
        case 7: // UInt32
        case 8: // UInt64
        case 13: // DateTime
            object.longValue = value as number | Long;
            break;
        case 9: // Float
            object.floatValue = value as number;
            break;
        case 10: // Double
            object.doubleValue = value as number;
            break;
        case 11: // Boolean
            object.booleanValue = value as boolean;
            break;
        case 12: // String
        case 14: // Text
        case 15: // UUID
            object.stringValue = value as string;
            break;
        case 16: // DataSet
            (object as IMetric).datasetValue = encodeDataSet(value as UDataSet);
            break;
        case 17: // Bytes
        case 18: // File
            (object as IMetric).bytesValue = value as Uint8Array;
            break;
        case 19: // Template
            (object as IMetric).templateValue = encodeTemplate(value as UTemplate);
            break;
        case 20: // PropertySet
            (object as IPropertyValue).propertysetValue = encodePropertySet(value as UPropertySet);
            break;
        case 21:
            (object as IPropertyValue).propertysetsValue = encodePropertySetList(value as UPropertySetList);
            break;
        case 22:
            (object as IMetric).bytesValue = encodeInt8Array(value as Array<number>);
            break;
        case 23:
            (object as IMetric).bytesValue = encodeInt16Array(value as Array<number>);
            break;
        case 24:
            (object as IMetric).bytesValue = encodeInt32Array(value as Array<number>);
            break;
        case 26:
            (object as IMetric).bytesValue = encodeUInt8Array(value as Array<number>);
            break;
        case 27:
            (object as IMetric).bytesValue = encodeUInt16Array(value as Array<number>);
            break;
        case 28:
            (object as IMetric).bytesValue = encodeUInt32Array(value as Array<number>);
            break;
        case 30:
            (object as IMetric).bytesValue = encodeFloatArray(value as Array<number>);
            break;
        case 31:
            (object as IMetric).bytesValue = encodeDoubleArray(value as Array<number>);
            break;
        case 32:
            (object as IMetric).bytesValue = encodeBooleanArray(value as Array<boolean>);
            break;
        case 33:
            (object as IMetric).bytesValue = encodeStringArray(value as Array<string>);
            break;
    } 
}

/** only used during decode functions */
function getValue<T extends UserValue> (type: number | null | undefined, object: IMetric | IPropertyValue): T | undefined | null {
    // TODO change type casts
    switch (type) {
        case 1: // Int8
        case 2: // Int16
        case 3: // Int32
            return new Int32Array([object.intValue!])[0] as T;
        case 5: // UInt8
        case 6: // UInt16
            return object.intValue as T;
        case 4: // Int64
            if (object.longValue instanceof Long) {
                return object.longValue.toSigned() as T;
            } else {
                return object.longValue as T;
            }
        case 7: // UInt32
            if (object.longValue instanceof Long) {
                return object.longValue.toInt() as T;
            } else {
                return object.longValue as T;
            }
        case 8: // UInt64
        case 13: // DateTime
            return object.longValue! as T;
        case 9: // Float
            return object.floatValue! as T;
        case 10: // Double
            return object.doubleValue! as T;
        case 11: // Boolean
            return object.booleanValue! as T;
        case 12: // String
        case 14: // Text
        case 15: // UUID
            return object.stringValue! as T;
        case 16: // DataSet
            return decodeDataSet((object as IMetric).datasetValue!) as T;
        case 17: // Bytes
        case 18: // File
            return (object as IMetric).bytesValue as T;
        case 19: // Template
            return decodeTemplate((object as IMetric).templateValue!) as T;
        case 20: // PropertySet
            return decodePropertySet((object as IPropertyValue).propertysetValue!) as T;
        case 21:
            return decodePropertySetList((object as IPropertyValue).propertysetsValue!) as T;
        case 22:
            return decodeInt8Array((object as IMetric).bytesValue!) as T;
        case 23:
            return decodeInt16Array((object as IMetric).bytesValue!) as T;
        case 24:
            return decodeInt32Array((object as IMetric).bytesValue!) as T;
        case 26:
            return decodeUInt8Array((object as IMetric).bytesValue!) as T;
        case 27:
            return decodeUInt16Array((object as IMetric).bytesValue!) as T;
        case 28:
            return decodeUInt32Array((object as IMetric).bytesValue!) as T;
        case 30:
            return decodeFloatArray((object as IMetric).bytesValue!) as T;
        case 31:
            return decodeDoubleArray((object as IMetric).bytesValue!) as T;
        case 32:
            return decodeBooleanArray((object as IMetric).bytesValue!) as T;
        case 33:
            return decodeStringArray((object as IMetric).bytesValue!) as T;
        default:
            return null;
    } 
}

function isSet<T> (value: T): value is Exclude<T, null | undefined> {
    return value !== null && value !== undefined;
}

function getDataSetValue (type: number | null | undefined, object: IDataSetValue): UDataSetValue {
    // sparkplug spec says that any normal metric value type can be used, but the `proto` definition only allows for a subset of these types
    switch (type) {
        // all the types that can be encoded in a datasetvalue
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9:
        case 10:
        case 11:
        case 12:
        case 13:
        case 14:
        case 15: {
            const val = getValue<UDataSetValue>(type, object);
            if (isSet(val)) return val;
        }

        default:
            throw new Error(`Invalid DataSetValue: ${JSON.stringify(object)}`);
    }
}

function getTemplateParamValue (type: number | null | undefined, object: IParameter): UParameter['value'] {
    // sparkplug spec says that any normal metric value type can be used, but the `proto` definition only allows for a subset of these types
    switch (type) {
        // all the types that can be encoded in a template parameter value
        case 1:
        case 2:
        case 3:
        case 4:
        case 5:
        case 6:
        case 7:
        case 8:
        case 9:
        case 10:
        case 11:
        case 12:
        case 13:
        case 14:
        case 15: {
            const val = getValue<UParameter['value']>(type, object);
            if (isSet(val)) return val;
        }

        default:
            throw new Error(`Invalid Parameter value: ${JSON.stringify(object)}`);
    }
}

/** transforms a user friendly type and converts it to its corresponding type code */
function encodeType(typeString: string): number {
    switch (typeString.toUpperCase()) {
        case "INT8":
            return 1;
        case "INT16":
            return 2;
        case "INT32":
        case "INT":
            return 3;
        case "INT64":
        case "LONG":
            return 4;
        case "UINT8":
            return 5;
        case "UINT16":
            return 6;
        case "UINT32":
            return 7;
        case "UINT64":
            return 8;
        case "FLOAT":
            return 9;
        case "DOUBLE":
            return 10;
        case "BOOLEAN":
            return 11;
        case "STRING":
            return 12;
        case "DATETIME":
            return 13;
        case "TEXT":
            return 14;
        case "UUID":
            return 15;
        case "DATASET":
            return 16;
        case "BYTES":
            return 17;
        case "FILE":
            return 18;
        case "TEMPLATE":
            return 19;
        case "PROPERTYSET":
            return 20;
        case "PROPERTYSETLIST":
            return 21;
        case "INT8ARRAY":
            return 22;
        case "INT16ARRAY":
            return 23;
        case "INT32ARRAY":
            return 24;
        case "UINT8ARRAY":
            return 26;
        case "UINT16ARRAY":
            return 27;
        case "UINT32ARRAY":
            return 28;
        case "FLOATARRAY":
            return 30;
        case "DOUBLEARRAY":
            return 31;
        case "BOOLEANARRAY":
            return 32;
        case "STRINGARRAY":
            return 33;
        default:
            return 0;
    }
}

/** transforms a type code into a user friendly type */
// @ts-expect-error TODO no consistent return
function decodeType (typeInt: number | null | undefined): TypeStr {
    switch (typeInt) {
        case 1:
            return "Int8";
        case 2:
            return "Int16";
        case 3:
            return "Int32";
        case 4:
            return "Int64";
        case 5:
            return "UInt8";
        case 6:
            return "UInt16";
        case 7:
            return "UInt32";
        case 8:
            return "UInt64";
        case 9:
            return "Float";
        case 10:
            return "Double";
        case 11:
            return "Boolean";
        case 12:
            return "String";
        case 13:
            return "DateTime";
        case 14:
            return "Text";
        case 15:
            return "UUID";
        case 16:
            return "DataSet";
        case 17:
            return "Bytes";
        case 18:
            return "File";
        case 19:
            return "Template";
        case 20:
            return "PropertySet";
        case 21:
            return "PropertySetList";
        case 22:
            return "Int8Array";
        case 23:
            return "Int16Array";
        case 24:
            return "Int32Array";
        case 26:
            return "UInt8Array";
        case 27:
            return "UInt16Array";
        case 28:
            return "UInt32Array";
        case 30:
            return "FloatArray";
        case 31:
            return "DoubleArray";
        case 32:
            return "BooleanArray";
        case 33:
            return "StringArray";
    }
}

function encodeTypes (typeArray: string[]): number[]  {
    var types: number[] = [];
    for (var i = 0; i < typeArray.length; i++) {
        types.push(encodeType(typeArray[i]));
    }
    return types;
}

function decodeTypes (typeArray: number[]): TypeStr[] {
    var types: TypeStr[] = [];
    for (var i = 0; i < typeArray.length; i++) {
        types.push(decodeType(typeArray[i]));
    }
    return types;
}

function encodeDataSet (object: UDataSet): ProtoRoot.org.eclipse.tahu.protobuf.Payload.DataSet {
    const num = object.numOfColumns,
        names = object.columns,
        types = encodeTypes(object.types),
        rows = object.rows,
        newDataSet = DataSet.create({
            "numOfColumns" : num, 
            "columns" : object.columns, 
            "types" : types 
        }),
        newRows = [];
    // Loop over all the rows
    for (let i = 0; i < rows.length; i++) {
        const newRow = Row.create(),
            row = rows[i],
            elements: IDataSetValue[] = [];
        // Loop over all the elements in each row
        // @ts-expect-error TODO check if num is set
        for (let t = 0; t < num; t++) {
            const newValue = DataSetValue.create();
            setValue(types[t], row[t], newValue);
            elements.push(newValue);
        }
        newRow.elements = elements;
        newRows.push(newRow);
    }
    newDataSet.rows = newRows as any;
    return newDataSet;
}

function decodeDataSet (protoDataSet: IDataSet): UDataSet {
    const protoTypes = protoDataSet.types!; // TODO check exists
    const dataSet: UDataSet = {
        types: decodeTypes(protoTypes),
        rows: [],
    };
    const types = decodeTypes(protoTypes),
        protoRows = protoDataSet.rows || [], // TODO check exists
        num = protoDataSet.numOfColumns;
    
    // Loop over all the rows
    for (var i = 0; i < protoRows.length; i++) {
        var protoRow = protoRows[i],
            protoElements = protoRow.elements || [], // TODO check exists
            rowElements: UDataSetValue[] = [];
        // Loop over all the elements in each row
        // @ts-expect-error TODO check exists
        for (var t = 0; t < num; t++) {
            rowElements.push(getDataSetValue(protoTypes[t], protoElements[t])!);
        }
        dataSet.rows.push(rowElements);
    }

    dataSet.numOfColumns = num;
    dataSet.types = types;
    dataSet.columns = protoDataSet.columns;

    return dataSet;
}

function encodeMetaData (object: IMetaData): ProtoRoot.org.eclipse.tahu.protobuf.Payload.MetaData {
    var metadata = MetaData.create(),
        isMultiPart = object.isMultiPart,
        contentType = object.contentType,
        size = object.size,
        seq = object.seq,
        fileName = object.fileName,
        fileType = object.fileType,
        md5 = object.md5,
        description = object.description;

    if (isMultiPart !== undefined && isMultiPart !== null) {
        metadata.isMultiPart = isMultiPart;
    }

    if (contentType !== undefined && contentType !== null) {
        metadata.contentType = contentType;
    }

    if (size !== undefined && size !== null) {
        metadata.size = size;
    }

    if (seq !== undefined && seq !== null) {
        metadata.seq = seq;
    }

    if (fileName !== undefined && fileName !== null) {
        metadata.fileName = fileName;
    }

    if (fileType !== undefined && fileType !== null) {
        metadata.fileType = fileType;
    }

    if (md5 !== undefined && md5 !== null) {
        metadata.md5 = md5;
    }

    if (description !== undefined && description !== null) {
        metadata.description = description;
    }

    return metadata;
}

function decodeMetaData (protoMetaData: IMetaData): IMetaData {
    var metadata: IMetaData = {},
        isMultiPart = protoMetaData.isMultiPart,
        contentType = protoMetaData.contentType,
        size = protoMetaData.size,
        seq = protoMetaData.seq,
        fileName = protoMetaData.fileName,
        fileType = protoMetaData.fileType,
        md5 = protoMetaData.md5,
        description = protoMetaData.description;

    if (isMultiPart !== undefined && isMultiPart !== null) {
        metadata.isMultiPart = isMultiPart;
    }

    if (contentType !== undefined && contentType !== null) {
        metadata.contentType = contentType;
    }

    if (size !== undefined && size !== null) {
        metadata.size = size;
    }

    if (seq !== undefined && seq !== null) {
        metadata.seq = seq;
    }

    if (fileName !== undefined && fileName !== null) {
        metadata.fileName = fileName;
    }

    if (fileType !== undefined && fileType !== null) {
        metadata.fileType = fileType;
    }

    if (md5 !== undefined && md5 !== null) {
        metadata.md5 = md5;
    }

    if (description !== undefined && description !== null) {
        metadata.description = description;
    }

    return metadata;
}

function encodePropertyValue (object: UPropertyValue): ProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertyValue {
    var type = encodeType(object.type),
        newPropertyValue = PropertyValue.create({
            "type" : type
        });

    if (object.value !== undefined && object.value === null) {
        newPropertyValue.isNull = true;
    }

    setValue(type, object.value, newPropertyValue);

    return newPropertyValue;
}

function decodePropertyValue (protoValue: IPropertyValue): UPropertyValue {
    const propertyValue: UPropertyValue = {
        // @ts-expect-error TODO check exists
        value: getValue(protoValue.type, protoValue),
        type: decodeType(protoValue.type),
    };

    if (protoValue.isNull !== undefined && protoValue.isNull === true) {
        propertyValue.value = null;
    } else {
        propertyValue.value = getValue(protoValue.type, protoValue)!;
    }

    propertyValue.type = decodeType(protoValue.type);

    return propertyValue;
}

function encodePropertySet (object: Record<string, UPropertyValue>): ProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertySet {
    const keys = [],
        values = [];

    for (var key in object) {
        if (object.hasOwnProperty(key)) {
            keys.push(key);
            values.push(encodePropertyValue(object[key]))  
        }
    }

    return PropertySet.create({
        "keys" : keys, 
        "values" : values
    });
}

function decodePropertySet (protoSet: IPropertySet): Record<string, UPropertyValue> {
    const propertySet: Record<string, UPropertyValue> = {},
        protoKeys = protoSet.keys || [], // TODO check exists
        protoValues = protoSet.values || []; // TODO check exists

    for (var i = 0; i < protoKeys.length; i++) {
        propertySet[protoKeys[i]] = decodePropertyValue(protoValues[i]);
    }

    return propertySet;
}

function encodePropertySetList (object: Record<string, UPropertyValue>[]): ProtoRoot.org.eclipse.tahu.protobuf.Payload.PropertySetList {
    const propertySets: IPropertySet[] = [];
    for (let i = 0; i < object.length; i++) {
        propertySets.push(encodePropertySet(object[i]));
    }
    return PropertySetList.create({
        "propertyset" : propertySets
    });
}

function decodePropertySetList (protoSetList: IPropertySetList): Record<string, UPropertyValue>[]  {
    const propertySets: Record<string, UPropertyValue>[] = [],
        protoSets = protoSetList.propertyset || []; // TODO check exists
    for (let i = 0; i < protoSets.length; i++) {
        propertySets.push(decodePropertySet(protoSets[i]));
    }
    return propertySets;
}

function encodeParameter (object: UParameter): ProtoRoot.org.eclipse.tahu.protobuf.Payload.Template.Parameter {
    const type = encodeType(object.type),
        newParameter = Parameter.create({
            "name" : object.name, 
            "type" : type
        });
    setValue(type, object.value, newParameter);
    return newParameter;
}

function decodeParameter (protoParameter: IParameter): UParameter {
    const protoType = protoParameter.type,
        parameter: UParameter = {
            value: getTemplateParamValue(protoType, protoParameter),
            type: decodeType(protoType),
        };

    parameter.name = protoParameter.name;
    parameter.type = decodeType(protoType);
    // @ts-expect-error TODO check exists
    parameter.value = getValue(protoType, protoParameter);

    return parameter;
}

function encodeTemplate (object: UTemplate): ProtoRoot.org.eclipse.tahu.protobuf.Payload.Template {
    let template = Template.create(),
        metrics = object.metrics,
        parameters = object.parameters,
        isDef = object.isDefinition,
        ref = object.templateRef,
        version = object.version;

    if (version !== undefined && version !== null) {
        template.version = version;
    }

    if (ref !== undefined && ref !== null) {
        template.templateRef = ref;
    }

    if (isDef !== undefined && isDef !== null) {
        template.isDefinition = isDef;
    }

    // Build up the metric
    if (object.metrics !== undefined && object.metrics !== null) {
        const newMetrics = []
            metrics = object.metrics;
        // loop over array of metrics
        for (let i = 0; i < metrics.length; i++) {
            newMetrics.push(encodeMetric(metrics[i]));
        }
        template.metrics = newMetrics;
    }

    // Build up the parameters
    if (object.parameters !== undefined && object.parameters !== null) {
        const newParameter = [];
        // loop over array of parameters
        for (let i = 0; i < object.parameters.length; i++) {
            newParameter.push(encodeParameter(object.parameters[i]));
        }
        template.parameters = newParameter;
    }

    return template;
}

function decodeTemplate (protoTemplate: ITemplate): UTemplate {
    const template: UTemplate = {},
        protoMetrics = protoTemplate.metrics,
        protoParameters = protoTemplate.parameters,
        isDef = protoTemplate.isDefinition,
        ref = protoTemplate.templateRef,
        version = protoTemplate.version;

    if (version !== undefined && version !== null) {
        template.version = version;    
    }

    if (ref !== undefined && ref !== null) {
        template.templateRef = ref;    
    }

    if (isDef !== undefined && isDef !== null) {
        template.isDefinition = isDef;    
    }

    // Build up the metric
    if (protoMetrics !== undefined && protoMetrics !== null) {
        const metrics = []
        // loop over array of proto metrics, decoding each one
        for (let i = 0; i < protoMetrics.length; i++) {
            metrics.push(decodeMetric(protoMetrics[i]));
        }
        template.metrics = metrics;
    }

    // Build up the parameters
    if (protoParameters !== undefined && protoParameters !== null) {
        const parameter: UParameter[] = [];
        // loop over array of parameters
        for (let i = 0; i < protoParameters.length; i++) {
            parameter.push(decodeParameter(protoParameters[i]));
        }
        template.parameters = parameter;
    }

    return template;
}

function encodeStringArray(array: Array<string>) {
    return Buffer.from(array.join("\0") + '\0', 'utf8');
}
  
function decodeStringArray(packedBytes: Uint8Array | null) {
    if (packedBytes === null) {
        return null;
    }
    return (Buffer.from(packedBytes).toString('utf8')).replace(/\0$/, '').split('\x00');
}

function encodeInt8Array(array: any[]) {
    return packValues(array, 'b');
}

function decodeInt8Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'b');
}

function encodeUInt8Array(array: any[]) {
    return packValues(array, 'B');
}

function decodeUInt8Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'B');
}

function encodeInt16Array(array: any[]) {
    return packValues(array, 'h');
}

function decodeInt16Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'h');
}

function encodeUInt16Array(array: any[]) {
    return packValues(array, 'H');
}

function decodeUInt16Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'H');
}

function encodeInt32Array(array: any[]) {
    return packValues(array, 'i');
}

function decodeInt32Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'i');
}

function encodeUInt32Array(array: any[]) {
    return packValues(array, 'I');
}

function decodeUInt32Array(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'I');
}

function encodeFloatArray(array: any[]) {
    return packValues(array, 'f');
}

function decodeFloatArray(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'f');
}

function encodeDoubleArray(array: any[]) {
    return packValues(array, 'd');
}

function decodeDoubleArray(array: Uint8Array | null) {
    if (array === null) {
        return null;
    }
    return unpackValues(array, 'd');
}
  
function unpackValues(packed_bytes: Uint8Array, format_specifier: string): number[] {
    const data_view = new DataView(packed_bytes.buffer, packed_bytes.byteOffset, packed_bytes.byteLength);
    const decodeFunc = {
        'b': data_view.getInt8.bind(data_view),
        'B': data_view.getUint8.bind(data_view),
        'h': data_view.getInt16.bind(data_view, 0, true),
        'H': data_view.getUint16.bind(data_view, 0, true),
        'i': data_view.getInt32.bind(data_view, 0, true),
        'I': data_view.getUint32.bind(data_view, 0, true),
        'f': data_view.getFloat32.bind(data_view, 0, true),
        'd': data_view.getFloat64.bind(data_view, 0, true),
    }[format_specifier];
    if (!decodeFunc) {
        throw new Error(`Unsupported format specifier: ${format_specifier}`);
    }
    const values = [];
    const typeSize = getTypeSize(format_specifier);
    for (let i = 0; i < packed_bytes.length / typeSize; i++) {
        values.push(decodeFunc(i * typeSize));
    }
    return values;
}

function packValues(values: any[], format_specifier: string): Uint8Array {
    const dataView = new DataView(new ArrayBuffer(values.length * getTypeSize(format_specifier)));
    let byteOffset = 0;
    for (let i = 0; i < values.length; i++) {
        const value = values[i];
        switch (format_specifier) {
            case 'b':
                dataView.setInt8(byteOffset, value);
                byteOffset += 1;
                break;
            case 'B':
                dataView.setUint8(byteOffset, value);
                byteOffset += 1;
                break;
            case 'h':
                dataView.setInt16(byteOffset, value, true);
                byteOffset += 2;
                break;
            case 'H':
                dataView.setUint16(byteOffset, value, true);
                byteOffset += 2;
                break;
            case 'i':
                dataView.setInt32(byteOffset, value, true);
                byteOffset += 4;
                break;
            case 'I':
                dataView.setUint32(byteOffset, value, true);
                byteOffset += 4;
                break;
            case 'f':
                dataView.setFloat32(byteOffset, value, true);
                byteOffset += 4;
                break;
            case 'd':
                dataView.setFloat64(byteOffset, value, true);
                byteOffset += 8;
                break;
            default:
                throw new Error(`Unsupported format specifier: ${format_specifier}`);
        }
    }
    return new Uint8Array(dataView.buffer);
}

function getTypeSize(format_specifier: string): number {
    const sizeMap: {[key: string]: number} = {
        'b': 1,
        'B': 1,
        'h': 2,
        'H': 2,
        'i': 4,
        'I': 4,
        'f': 4,
        'd': 8,
    };
    const size = sizeMap[format_specifier];
    if (!size) {
        throw new Error(`Unsupported format specifier: ${format_specifier}`);
    }
    return size;
}

function encodeBooleanArray(booleanArray: boolean[]): Uint8Array {
    // calculate the number of packed bytes required
    const packedBytesCount = Math.ceil(booleanArray.length / 8);

    // convert the boolean array into a packed byte array
    const packedBytes = new Uint8Array(packedBytesCount);

    for (let i = 0; i < booleanArray.length; i++) {
        const value = booleanArray[i];
        const byteIndex = Math.floor(i / 8);
        const bitIndex = i % 8;
        packedBytes[byteIndex] |= (value ? 1 : 0) << bitIndex;
    }

    // return the packed bytes preceded by a 4-byte integer representing the number of boolean values
    const lengthBytes = new Uint8Array(new Uint32Array([booleanArray.length]).buffer);
    const result = new Uint8Array(lengthBytes.length + packedBytes.length);
    result.set(lengthBytes);
    result.set(packedBytes, lengthBytes.length);

    return result;
}

function decodeBooleanArray(packedBytes: Uint8Array): boolean[] {
    // extract the length of the boolean array from the first 4 bytes of the packed bytes
    const lengthBytes = packedBytes.slice(0, 4);
    const length = new Uint32Array(lengthBytes.buffer)[0];

    // create a boolean array of the appropriate length
    const booleanArray = new Array<boolean>(length);

    // iterate over each bit in the packed bytes and set the corresponding boolean value in the boolean array
    for (let i = 0; i < length; i++) {
        const byteIndex = Math.floor(i / 8);
        const bitIndex = i % 8;
        const mask = 1 << bitIndex;
        booleanArray[i] = (packedBytes[byteIndex + 4] & mask) !== 0;
    }

    return booleanArray;
}

function encodeMetric (metric: UMetric): ProtoRoot.org.eclipse.tahu.protobuf.Payload.Metric {
    const newMetric = Metric.create({
            name : metric.name
        }),
        value = metric.value,
        datatype = encodeType(metric.type),
        alias = metric.alias,
        isHistorical = metric.isHistorical,
        isTransient = metric.isTransient,
        metadata = metric.metadata,
        timestamp = metric.timestamp,
        properties = metric.properties;
    
    // Get metric type and value
    newMetric.datatype = datatype;
    setValue(datatype, value, newMetric);

    if (timestamp !== undefined && timestamp !== null) {
        newMetric.timestamp = timestamp;
    }

    if (alias !== undefined && alias !== null) {
        newMetric.alias = alias;
    }

    if (isHistorical !== undefined && isHistorical !== null) {
        newMetric.isHistorical = isHistorical;
    }

    if (isTransient !== undefined && isTransient !== null) {
        newMetric.isTransient = isTransient;
    }

    if (value !== undefined && value === null) {
        newMetric.isNull = true;
    }

    if (metadata !== undefined && metadata !== null) {
        newMetric.metadata = encodeMetaData(metadata);
    }

    if (properties !== undefined && properties !== null) {
        newMetric.properties = encodePropertySet(properties) as any;
    }

    return newMetric;
}

function decodeMetric (protoMetric: Partial<IMetric>): UMetric {
    const metric: UMetric = {
        // @ts-expect-error TODO check exists
        value: getValue(protoMetric.datatype, protoMetric),
        type: decodeType(protoMetric.datatype)
    };

    if (protoMetric.hasOwnProperty("name")) {
        metric.name = protoMetric.name;
    }

    if (protoMetric.hasOwnProperty("isNull") && protoMetric.isNull === true) {
        metric.value = null;
    } else {
        // @ts-expect-error TODO check exists
        metric.value = getValue(protoMetric.datatype, protoMetric);
    }

    if (protoMetric.hasOwnProperty("timestamp")) {
        metric.timestamp = protoMetric.timestamp;
    }

    if (protoMetric.hasOwnProperty("alias")) {
        metric.alias = protoMetric.alias;
    }

    if (protoMetric.hasOwnProperty("isHistorical")) {
        metric.isHistorical = protoMetric.isHistorical;
    }

    if (protoMetric.hasOwnProperty("isTransient")) {
        metric.isTransient = protoMetric.isTransient;
    }

    if (protoMetric.hasOwnProperty("metadata") && protoMetric.metadata) {
        metric.metadata = decodeMetaData(protoMetric.metadata);
    }

    if (protoMetric.hasOwnProperty("properties") && protoMetric.properties) {
        metric.properties = decodePropertySet(protoMetric.properties);
    }

    return metric;
}

export function encodePayload(object: UPayload): Uint8Array {
    var payload = Payload.create({
        "timestamp" : object.timestamp
    });

    // Build up the metric
    if (object.metrics !== undefined && object.metrics !== null) {
        var newMetrics = [],
            metrics = object.metrics;
        // loop over array of metric
        for (var i = 0; i < metrics.length; i++) {
            newMetrics.push(encodeMetric(metrics[i]));
        }
        payload.metrics = newMetrics as any;
    }

    if (object.seq !== undefined && object.seq !== null) {
        payload.seq = object.seq;
    }

    if (object.uuid !== undefined && object.uuid !== null) {
        payload.uuid = object.uuid;
    }

    if (object.body !== undefined && object.body !== null) {
        payload.body = object.body;
    }

    return Payload.encode(payload).finish();
}

export function decodePayload(proto: Uint8Array | Reader): UPayload {
    var sparkplugPayload = Payload.decode(proto),
        payload: UPayload = {};

    if (sparkplugPayload.hasOwnProperty("timestamp")) {
        payload.timestamp = sparkplugPayload.timestamp;
    }

    if (sparkplugPayload.hasOwnProperty("metrics")) {
        const metrics: UMetric[] = [];
        for (var i = 0; i < sparkplugPayload.metrics.length; i++) {
            metrics.push(decodeMetric(sparkplugPayload.metrics[i]));
        }
        payload.metrics = metrics;
    }

    if (sparkplugPayload.hasOwnProperty("seq")) {
        payload.seq = sparkplugPayload.seq;
    }

    if (sparkplugPayload.hasOwnProperty("uuid")) {
        payload.uuid = sparkplugPayload.uuid;
    }

    if (sparkplugPayload.hasOwnProperty("body")) {
        payload.body = sparkplugPayload.body;
    }

    return payload;
}
