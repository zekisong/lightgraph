package raft.log;

public class Test {
    public static void main(String[] args) throws InterruptedException {

 /*       LogSystem logSystem = new LogSystem(null, "/tmp/test", 1024 * 1024, new TimeWheel(1000, 10));
        for (int i = 0; i < 1000; i++) {
            byte[] record = ("data" + i).getBytes();
            logSystem.append(i, record);
        }
        long start = System.currentTimeMillis();
        System.out.println(logSystem.getLogEdit(0));
        long end = System.currentTimeMillis();
        System.out.println(end - start);*/

      /*  ConcurrentSkipListMap map = new ConcurrentSkipListMap();
        map.put(4, 1);
        map.put(3, 1);
        map.put(1, 1);
        map.put(2, 1);
        map.put(8, 1);
        System.out.println(map.floorEntry(5));
        for (Object obj : map.tailMap(4).keySet())
            System.out.println(obj);*/


       /* FileChannel dataChannel = FileChannel.open(new File("/tmp/dd").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        ByteBuffer buffer = ByteBuffer.allocate(10);
        buffer.put("0123456782".getBytes());
        buffer.flip();
        dataChannel.position(dataChannel.size() - 10);
        dataChannel.write(buffer);
        dataChannel.force(true);
        System.out.println(dataChannel.size());
        dataChannel.truncate(10);
        dataChannel.force(true);*/
    }
}
