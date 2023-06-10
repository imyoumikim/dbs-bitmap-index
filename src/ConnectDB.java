import java.io.*;
import java.nio.ByteBuffer;
import java.sql.*;
import java.util.Random;

public class ConnectDB {
    private final String url = "jdbc:mysql://localhost:3306/diagnostics?verifyServerCertificate=false&useSSL=true"; // Port 번호 3306, DB명 diagnostics
    private final String userName = "root";
    private final String password = "1234";

    private Connection conn = null;
    private PreparedStatement pstmt = null;
    private Statement stmt = null;
    private ResultSet rset = null;

    private static final int ROW_COUNT = 100;  // 전체 레코드 수를 여기서 수정하시면 됩니다.
    // 랜덤 값을 생성할 범위
    private static final String[] GENDER_VALUES = {"M", "F"};
    private static final int[] AGE_VALUES = {1, 2, 3, 4, 5, 6, 7, 8};
    private static final int[] BMI_VALUES = {1, 2, 3, 4, 5};
    private static final int[] SMOKE_VALUES = {1, 2, 3};
    private static final int[] BP_VALUES = {1, 2, 3, 4};
    private static final int[] DM_VALUES = {1, 2, 3};

    public void createTable() throws SQLException {
        // SQL문 작성
        String query = "CREATE TABLE medical_test("
                + "patient_id INT NOT NULL AUTO_INCREMENT,"
                + "gender VARCHAR(1) NULL,"
                + "age_g INT NULL,"
                + "bmi_g INT NULL,"
                + "smoke INT NULL,"
                + "bp INT NULL,"
                + "dm INT NULL,"
                + "PRIMARY KEY (patient_id));";
        try {
            // DB 연결
            conn = DriverManager.getConnection(url, userName, password);

            stmt = conn.createStatement();
            stmt.executeUpdate(query);
            System.out.println("테이블 생성 완료!");
        }catch (Exception e){
            System.out.println("테이블을 생성할 수 없음!");
            e.printStackTrace();
        }finally {
            stmt.close();
            conn.close();
        }
    }
    public void insertRows() throws SQLException {   // 행 삽입
        // SQL문 작성
        String query = "INSERT INTO medical_test (gender, age_g, bmi_g, smoke, bp, dm) VALUES (?, ?, ?, ?, ?, ?)";
        try{
            // DB 연결
            conn = DriverManager.getConnection(url, userName, password);

            pstmt = conn.prepareStatement(query);
            Random random = new Random();
            System.out.println("랜덤값으로 이루어진 행 "+ROW_COUNT+"개 삽입 시작!");

            for (int i = 0; i < ROW_COUNT; i++) {
                pstmt.setString(1, getRandomValue(GENDER_VALUES, random));
                pstmt.setInt(2, getRandomValue(AGE_VALUES, random));
                pstmt.setInt(3, getRandomValue(BMI_VALUES, random));
                pstmt.setInt(4, getRandomValue(SMOKE_VALUES, random));
                pstmt.setInt(5, getRandomValue(BP_VALUES, random));
                pstmt.setInt(6, getRandomValue(DM_VALUES, random));

                pstmt.executeUpdate();
            }
            System.out.println("medical_test 테이블에 행 "+ROW_COUNT+"개가 삽입 완료되었습니다.");

        }catch (SQLException e) {
            System.out.println("행 삽입 중 문제가 발생하였습니다.");
            e.printStackTrace();
        } finally {
            pstmt.close();
            conn.close();
        }
    }
    private static String getRandomValue(String[] array, Random random) {
        return array[random.nextInt(array.length)];
    }
    private static int getRandomValue(int[] array, Random random) {
        return array[random.nextInt(array.length)];
    }

    // 비트맵을 생성할 컬럼 정보와 해당 컬럼들의 도메인 사이즈
    private static final String[] BITMAP_COLUMNS = {"gender", "bmi_g", "dm"};
    private static final int[] DOMAIN_SIZES = {GENDER_VALUES.length, BMI_VALUES.length, DM_VALUES.length};  // {2,3,5}
    public void generateBitmapIndex() throws SQLException {  // "gender", "bmi_g", "dm"에 대해 bitmap index 생성
        try{
            // DB 연결
            conn = DriverManager.getConnection(url, userName, password);

            for (int i = 0; i < BITMAP_COLUMNS.length; i++) {   // 3번 반복 = 비트맵을 만들 컬럼 개수 만큼
                String column = BITMAP_COLUMNS[i];
                int domainSize = DOMAIN_SIZES[i];

                String query = "SELECT " + column + " FROM medical_test";
                pstmt = conn.prepareStatement(query);
                rset = pstmt.executeQuery();    // 해당 컬럼에 대한 select 연산 결과를 rset에 저장

                StringBuilder[] bitString = new StringBuilder[domainSize];
                for(int j = 0; j < domainSize; j++)
                    bitString[j] = new StringBuilder();  // 객체 배열 초기화

                // 행 읽어오면서 bit string 생성
                if(i == 0){    // gender은 "M", "F" 값을 가짐
                    while (rset.next()) {
                        String value = rset.getString(column);  // 해당 행의 컬럼 값
                        appendBitString(bitString, value);
                    }
                }else{
                    while (rset.next()) {
                        int value = rset.getInt(column);  // 해당 행의 컬럼 값
                        appendBitString(bitString, value);
                    }
                }

                // CSV 파일에 비트 문자열 쓰기
                try (FileWriter writer = new FileWriter(column + "_bitmap_index.csv")) {
                    for(int j = 0; j < bitString.length; j++){  // 파일에 첫 행에는 각 컬럼값을 쓰기 ex) M, F
                        if(i == 0){ // gender
                            writer.write(GENDER_VALUES[j]);
                            if (j != bitString.length - 1)
                                writer.write(",");
                        }
                        else if (i == 1){   // bmi_g
                            writer.write(Integer.toString(BMI_VALUES[j]));
                            if (j != bitString.length - 1)
                                writer.write(",");
                        }
                        else if(i == 2){    // dm
                            writer.write(Integer.toString(DM_VALUES[j]));
                            if (j != bitString.length - 1)
                                writer.write(",");
                        }
                    }
                    writer.write("\n"); // 다음 행으로 넘어감
                    for(int j = 0; j < bitString.length; j++){  // 각 biString을 파일에 쓰기
                        writer.write(bitString[j].toString());
                        if (j != bitString.length - 1)
                            writer.write(",");
                    }

                } catch (IOException e) {
                    System.out.println("비트맵 인덱스 생성에 문제가 발생하였습니다.");
                    e.printStackTrace();
                }
            }
            System.out.println("성별, 체질량지수 그룹, 당뇨병 유무, 총 3개의 컬럼에 대한 bitmap index가 생성되었습니다.");
        }catch (SQLException e){
            e.printStackTrace();
        }finally {
            rset.close();
            pstmt.close();
            conn.close();
        }
    }
    private static void appendBitString(StringBuilder[] bitString, String value){
        if(value.equals("M")){
            bitString[0].append("1");   // bitString[0]은 남성
            bitString[1].append("0");   // bitString[1]은 여성
        }else if(value.equals("F")){
            bitString[0].append("0");
            bitString[1].append("1");
        }
    }
    private static void appendBitString(StringBuilder[] bitString, int value) {
        for (int i = 0; i < bitString.length; i++) {
            if (i == (value - 1)) { // ex) value가 3이면 bitString[2]에 1을 씀
                bitString[i].append("1");
            } else {
                bitString[i].append("0");
            }
        }
    }
    private static final int BUFFER_SIZE = 80 * 1024; // 80KB
    private static final int BLOCK_SIZE = 4 * 1024; // 4KB
    public void firstQuery(){    // 고도비만(5) OR 당뇨(3)인 환자의 수 COUNT(*)
        int sqlCountResult = 0; // select count(*)의 결과 저장
        int location = 0;   // 파일에서 데이터를 읽어올 때 현재 읽어온 바이트의 개수
        try{
            ByteBuffer[] bufferPages = new ByteBuffer[2];   // bmi_g에 대한 버퍼 페이지, dm에 대한 버퍼 페이지
            for (int i = 0; i < bufferPages.length; i++) {
                bufferPages[i] = ByteBuffer.allocate(BUFFER_SIZE);  // 버퍼의 각 페이지를 80KB에 맞게 할당
            }

            BufferedReader bmiIndexReader = new BufferedReader(new FileReader("bmi_g_bitmap_index.csv"));
            BufferedReader dmIndexReader = new BufferedReader(new FileReader("dm_bitmap_index.csv"));
            bmiIndexReader.readLine();  // 둘다 첫 행 제거
            dmIndexReader.readLine();

            byte[] blockForBmi = new byte[BLOCK_SIZE];
            byte[] blockForDm = new byte[BLOCK_SIZE];
            byte[] result = new byte[BLOCK_SIZE];

            String bmiIndex5 = bmiIndexReader.readLine().split(",")[4];  // 5개 그룹 중 고도비만인 사람의 비트맵 인덱스 가져오기
            String dmIndex3 = dmIndexReader.readLine().split(",")[2]; // 3개 그룹 중 당뇨인 사람의 비트맵 인덱스 가져오기

            while(location < ROW_COUNT){    // 비트맵 인덱스의 끝(즉, 행의 총 개수)에 다다르기 전까지 반복
                if(bufferPages[0].remaining() == 0 && bufferPages[1].remaining() == 0) { // 버퍼에 빈 자리가 없으면 버퍼를 비움
                    bufferPages[0].clear();
                    bufferPages[1].clear();
                }else {
                    for (int i = 0; i < BLOCK_SIZE; i++) {  // String bmiIndex = "11000..." -> 블록 사이즈 = 4KB짜리 바이트 배열에 저장
                        if (location < bmiIndex5.length() && location < dmIndex3.length()) {
                            char cb = bmiIndex5.charAt(location);
                            char cd = dmIndex3.charAt(location);
                            blockForBmi[i] = (byte) (cb == '1' ? 1 : 0);
                            blockForDm[i] = (byte) (cd == '1' ? 1 : 0);     // 문자열을 byte 배열로 변환

                            result[i] = (byte) (blockForBmi[i] | blockForDm[i]);
                            if (result[i] == 1)
                                sqlCountResult += 1;   // 1의 개수 세기
                            location += 1;  // 현재 읽고 있는 비트맵 인덱스의 위치 업데이트
                        }
                        else{
                            break;
                        }
                    }
                    bufferPages[0].put(blockForBmi);    // 두 버퍼에 블록 단위로 데이터 저장
                    bufferPages[1].put(blockForDm);
                }
            }
            System.out.println("고도비만이거나 당뇨를 앓고 있는 환자의 수 COUNT 결과: " + sqlCountResult);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void secondQuery(){    // 당뇨 전단계인 여성 환자의 수 COUNT(*)
        int sqlCountResult = 0; // select count(*)의 결과 저장
        int location = 0;   // 파일에서 데이터를 읽어올 때 현재 읽어온 바이트의 개수
        try{
            ByteBuffer[] bufferPages = new ByteBuffer[2];   // gender에 대한 버퍼 페이지, dm에 대한 버퍼 페이지
            for (int i = 0; i < bufferPages.length; i++) {
                bufferPages[i] = ByteBuffer.allocate(BUFFER_SIZE);  // 버퍼의 각 페이지를 80KB에 맞게 할당
            }

            BufferedReader genderIndexReader = new BufferedReader(new FileReader("gender_bitmap_index.csv"));
            BufferedReader dmIndexReader = new BufferedReader(new FileReader("dm_bitmap_index.csv"));
            genderIndexReader.readLine();  // 둘다 첫 행 제거
            dmIndexReader.readLine();

            byte[] blockForGender = new byte[BLOCK_SIZE];
            byte[] blockForDm = new byte[BLOCK_SIZE];
            byte[] result = new byte[BLOCK_SIZE];

            String genderLine = genderIndexReader.readLine();     // 파일의 두번째 행을 읽어서 genderLine에 저장
            String femaleIndex = genderLine.split(",")[1];  // 여성의 비트맵 인덱스 가져오기

            String dmLine = dmIndexReader.readLine();
            String dmIndex2 = dmLine.split(",")[1]; // 3개 그룹 중 당뇨 전 단계인 사람의 비트맵 인덱스 가져오기

            while(location < ROW_COUNT){    // 비트맵 인덱스의 끝(즉, 행의 총 개수)에 다다르기 전까지 반복
                if(bufferPages[0].remaining() == 0 && bufferPages[1].remaining() == 0) { // 버퍼에 빈 자리가 없으면 버퍼를 비움
                    bufferPages[0].clear();
                    bufferPages[1].clear();
                }else {
                    for (int i = 0; i < BLOCK_SIZE; i++) {  // String bmiIndex = "11000..." -> 블록 사이즈 = 4KB짜리 바이트 배열에 저장
                        if (location < femaleIndex.length() && location < dmIndex2.length()) {
                            char cf = femaleIndex.charAt(location);
                            char cd = dmIndex2.charAt(location);
                            blockForGender[i] = (byte) (cf == '1' ? 1 : 0);
                            blockForDm[i] = (byte) (cd == '1' ? 1 : 0);     // 문자열을 byte 배열로 변환

                            result[i] = (byte) (blockForGender[i] & blockForDm[i]);
                            if (result[i] == 1)
                                sqlCountResult += 1;   // 1의 개수 세기
                            location += 1;  // 현재 읽고 있는 비트맵 인덱스의 위치 업데이트
                        }
                        else{
                            break;
                        }
                    }
                    bufferPages[0].put(blockForGender);    // 두 버퍼에 블록 단위로 데이터 저장
                    bufferPages[1].put(blockForDm);
                }
            }
            System.out.println("당뇨 전 단계인 여성 환자의 수 COUNT 결과: " + sqlCountResult);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void thirdQuery() throws SQLException {    // 비만인 남성의 patient_id 출력
        int location = 0;   // 파일에서 데이터를 읽어올 때 현재 읽어온 바이트의 개수
        byte[] resultBitArr = new byte[ROW_COUNT];

        try{
            ByteBuffer[] bufferPages = new ByteBuffer[2];   // gender에 대한 버퍼 페이지, bmi_g에 대한 버퍼 페이지
            for (int i = 0; i < bufferPages.length; i++) {
                bufferPages[i] = ByteBuffer.allocate(BUFFER_SIZE);  // 버퍼의 각 페이지를 80KB에 맞게 할당
            }

            BufferedReader genderIndexReader = new BufferedReader(new FileReader("gender_bitmap_index.csv"));
            BufferedReader bmiIndexReader = new BufferedReader(new FileReader("bmi_g_bitmap_index.csv"));
            genderIndexReader.readLine();  // 둘다 첫 행 제거
            bmiIndexReader.readLine();

            byte[] blockForGender = new byte[BLOCK_SIZE];
            byte[] blockForBmi = new byte[BLOCK_SIZE];
            byte result;

            String genderLine = genderIndexReader.readLine();     // 파일의 두번째 행을 읽어서 genderLine에 저장
            String maleIndex = genderLine.split(",")[0];  // 남성의 비트맵 인덱스 가져오기

            String bmiLine = bmiIndexReader.readLine();
            String bmiIndex4 = bmiLine.split(",")[3]; // 5개 그룹 중 비만인 사람의 비트맵 인덱스 가져오기

            while(location < ROW_COUNT){    // 비트맵 인덱스의 끝(즉, 행의 총 개수)에 다다르기 전까지 반복
                if(bufferPages[0].remaining() == 0 && bufferPages[1].remaining() == 0) { // 버퍼에 빈 자리가 없으면 버퍼를 비움
                    bufferPages[0].clear();
                    bufferPages[1].clear();
                }else {
                    for (int i = 0; i < BLOCK_SIZE; i++) {  // String bmiIndex = "11000..." -> 블록 사이즈 = 4KB짜리 바이트 배열에 저장
                        if (location < maleIndex.length() && location < bmiIndex4.length()) {
                            char cf = maleIndex.charAt(location);
                            char cd = bmiIndex4.charAt(location);
                            blockForGender[i] = (byte) (cf == '1' ? 1 : 0);
                            blockForBmi[i] = (byte) (cd == '1' ? 1 : 0);     // 문자열을 byte 배열로 변환
                            result = (byte) (blockForGender[i] & blockForBmi[i]);
                            resultBitArr[location] = result;
                            location += 1;  // 현재 읽고 있는 비트맵 인덱스의 위치 업데이트
                        } else{
                            break;
                        }
                    }
                    bufferPages[0].put(blockForGender);    // 두 버퍼에 블록 단위로 데이터 저장
                    bufferPages[1].put(blockForBmi);
                }
            }
            conn = DriverManager.getConnection(url, userName, password);
            String query = "SELECT patient_id from medical_test;";
            pstmt = conn.prepareStatement(query);
            rset = pstmt.executeQuery();    // 해당 컬럼에 대한 select 연산 결과를 rset에 저장

            int idx = 0;
            System.out.print("비만인 남성의 patient_id: ");
            while (rset.next()){
                if(resultBitArr[idx++] == 1){
                    System.out.print(rset.getInt(1));
                    if(idx < ROW_COUNT - 1) System.out.print(" ");
                }
            }
            System.out.println();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            rset.close();;
            pstmt.close();
            conn.close();
        }
    }
}
