import java.sql.SQLException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws SQLException {
        Scanner scanner = new Scanner(System.in);
        ConnectDB myDB = new ConnectDB();

        int menu;
        System.out.println("병원에 내원한 환자에 대한 기초 진단 검사 결과 조회 프로그램을 시작합니다. 아래 번호를 차례대로 선택해주십시오.");
        while(true){
            System.out.print("1: 테이블 생성\n" +
                    "2: 랜덤 값으로 이루어진 레코드 자동 삽입\n" +
                    "3: 비트맵 인덱스를 이용한 질의 처리\n" +
                    "4: 프로그램 종료\n"+
                    "번호를 선택해주세요 >> ");
            menu = scanner.nextInt();

            if (menu == 1) {
                myDB.createTable(); // 테이블 생성
                System.out.println("위와 같이 7개의 컬럼을 가진 테이블 'medical_test'가 생성되었습니다.\n" +
                        "---------------------------------------------------------\n" +
                        "| patient_id | gender | age_g | bmi_g | smoke | bp | dm |\n" +
                        "---------------------------------------------------------\n" +
                        "각 컬럼에 대한 설명은 다음과 같습니다.\n" +
                        "patient_id: 환자 고유의 ID(PK)\n" +
                        "gender: 성별. M 또는 F의 문자. 남성(M), 여성(F)\n" +
                        "age_g: 연령 그룹. 1~8 사이의 정수. 만 19세 이하(1), 만 20~29세 이하(2), 만 30~39세(3), …, 만 80세 이상(8)\n" +
                        "bmi_g: 체질량지수 그룹. 1~5 사이의 정수. 저체중(1), 표준(2), 과체중(3), 비만(4), 고도비만(5)\n" +
                        "smoke: 흡연 여부. 1~3 사이의 정수. 비흡연(1), 과거 흡연(2), 현재 흡연(3)\n" +
                        "bp: 고혈압 여부. 1~4 사이의 정수. 정상(1), 주의 혈압(2), 고혈압 전단계(3), 고혈압(4)\n" +
                        "dm: 당뇨병 유무. 1~3 사이의 정수. 정상(1), 당뇨 전 단계(2), 당뇨(3)\n");
            }else if(menu == 2){
                myDB.insertRows();  // 행 삽입
            } else if (menu == 3) {
                myDB.generateBitmapIndex(); // 비트맵 인덱스 생성
                System.out.println("비트맵 인덱스를 이용하여 수행할 수 있는 질의는 다음과 같습니다.\n"+
                        "> 1: 고도비만이거나 당뇨를 앓고 있는 환자의 수\n"+
                        "> 2: 당뇨 전단계인 여성 환자의 수\n"+
                        "> 3: 비만인 남성 환자의 수\n"+
                        "> 4: 질의 종료");

                while(true){
                    System.out.print("질의를 선택해주세요 (숫자 입력) >> ");
                    int sql = scanner.nextInt();

                    if(sql == 1){   // 고도비만이거나 당뇨를 앓고 있는 환자의 수
                        myDB.firstQuery();
                    }else if(sql == 2){ // 당뇨 전단계인 여성 환자의 수
                        myDB.secondQuery();
                    }else if(sql == 3){ // 비만인 남성 환자의 id
                        myDB.thirdQuery();
                    }else{
                        System.out.println("질의가 종료되었습니다.");
                        break;
                    }
                }

            } else if (menu == 4){
                System.out.println("프로그램이 종료되었습니다.");
                break;
            }else{
                System.out.println("잘못된 번호입니다.");
            }
        }
    }
}