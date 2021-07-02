package site.wetsion.framework.infrastucture.cache.util;

import com.dyuproject.protostuff.LinkedBuffer;
import com.dyuproject.protostuff.ProtostuffIOUtil;
import com.dyuproject.protostuff.Schema;
import com.dyuproject.protostuff.runtime.RuntimeSchema;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.List;

/**
 * @author <a href="mailto:weixin@cai-inc.com">霜华</a>
 * @date 2021/6/30 5:54 PM
 **/
@Slf4j
public class SerializeUtil {

    private final static int SIZE = 1024;

    @Deprecated
    public static byte[] javaSerialize(Object object) {

        ObjectOutputStream oos = null;
        ByteArrayOutputStream baos = null;
        try {
            // 序列化
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(object);
            byte[] bytes = baos.toByteArray();
            return bytes;
        } catch (Exception e) {
            log.debug("[wetsion-infrastructure][SerializeUtil][javaUnserialize] serialize failed!",e);
        }
        return null;
    }

    @Deprecated
    public static Object javaUnserialize(byte[] bytes) {
        ByteArrayInputStream bais = null;
        try {
            // 反序列化
            bais = new ByteArrayInputStream(bytes);
            ObjectInputStream ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (Exception e) {
            log.debug("[wetsion-infrastructure][SerializeUtil][javaUnserialize] serialize failed!",e);
        }
        return null;
    }

    public static <T> byte[] serialize(T obj) {
        if (obj == null) {
            throw new RuntimeException("序列化对象(" + obj + ")!");
        }
        @SuppressWarnings("unchecked") LinkedBuffer buffer = LinkedBuffer.allocate(SIZE);
        byte[] protostuff = null;
        try {
            Schema<T> schema = (Schema<T>) RuntimeSchema.getSchema(obj.getClass());
            protostuff = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } catch (Exception e) {
            log.debug("[wetsion-infrastructure][SerializeUtil] serialize {} failed", obj,e);
        } finally {
            buffer.clear();
        }
        return protostuff;
    }

    public static <T> T deserialize(byte[] paramArrayOfByte, Class<T> targetClass) {
        if (paramArrayOfByte == null || paramArrayOfByte.length == 0) {
            throw new RuntimeException("反序列化对象发生异常,byte序列为空!");
        }
        T instance = null;
        try {
            instance = targetClass.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
            log.debug("[wetsion-infrastructure][deserialize] deserialize {},{} failed",paramArrayOfByte,targetClass);
        } finally {
            Schema<T> schema = RuntimeSchema.getSchema(targetClass);
            if(instance == null) {
                instance = schema.newMessage();
            }
            ProtostuffIOUtil.mergeFrom(paramArrayOfByte, instance, schema);
        }

        return instance;
    }

    public static <T> byte[] serializeList(List<T> objList) {
        if (objList == null || objList.isEmpty()) {
            throw new RuntimeException("序列化对象列表(" + objList + ")参数异常!");
        }
        @SuppressWarnings("unchecked")
        Schema<T> schema = (Schema<T>) RuntimeSchema.getSchema(objList.get(0).getClass());
        LinkedBuffer buffer = LinkedBuffer.allocate(SIZE);
        byte[] protostuff = null;
        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            ProtostuffIOUtil.writeListTo(bos, objList, schema, buffer);
            protostuff = bos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("序列化对象列表(" + objList + ")发生异常!", e);
        } finally {
            buffer.clear();
            try {
                if(bos!=null){
                    bos.close();
                }
            } catch (IOException e) {
                log.debug("关闭IO资源发生异常,序列化对象列表({})",objList,e);
            }
        }

        return protostuff;
    }

    public static <T> List<T> deserializeList(byte[] paramArrayOfByte, Class<T> targetClass) {
        if (paramArrayOfByte == null || paramArrayOfByte.length == 0) {
            throw new RuntimeException("反序列化对象发生异常,byte序列为空!");
        }

        Schema<T> schema = RuntimeSchema.getSchema(targetClass);
        List<T> result = null;
        try {
            result = ProtostuffIOUtil.parseListFrom(new ByteArrayInputStream(paramArrayOfByte), schema);
        } catch (IOException e) {
            throw new RuntimeException("反序列化对象列表发生异常!",e);
        }
        return result;
    }

}
