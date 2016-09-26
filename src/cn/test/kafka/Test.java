package cn.test.kafka;

public class Test {
	public static void main(String[] args) {
		System.out.println("gj");
		Thread t = new Thread(new KafkaConsumer());
		t.start();
	}
}
