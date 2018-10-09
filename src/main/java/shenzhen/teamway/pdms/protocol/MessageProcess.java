
package shenzhen.teamway.pdms.protocol;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import shenzhen.teamway.pdg.protobuf.MessageProtobuf;
import shenzhen.teamway.pdms.config.CodeDeviceType;
import shenzhen.teamway.pdms.config.GlobalData;
import shenzhen.teamway.pdms.db.pojo.*;
import shenzhen.teamway.pdms.mg.ClientManager;
import shenzhen.teamway.pdms.server.NettyServerClientHandler;
import shenzhen.teamway.pdg.protobuf.MessageProtobuf.*;
import static shenzhen.teamway.pdg.protobuf.MessageProtobuf.CommandType.*;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;


public class MessageProcess {

    private Logger log = LoggerFactory.getLogger(NettyServerClientHandler.class);

    // 处理注册消息
    public PDGMessage processRegister(PDGMessage register) {
        String password = register.getRegister().getPassword();
        PDGHeader header = register.getPDGHeader();
        log.info("注册密码: [" + password.substring(0,3) + "**********]");

        boolean success = password.equals(GlobalData.getInstance().getRegisterPassword());
        log.info("客户端[" + header.getFromAddr() + "][" + header.getStationCode() + "]连接" + (success ? "成功" : "失败") + "。");

        String uuid = "";
        if (success) {
            uuid = UUID.randomUUID().toString();
            uuid = uuid.replace("-", "");
            uuid = uuid.toUpperCase();
            log.info("分配会话: " + uuid);
        }

        PDGMessage.Builder builder = PDGMessage.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), uuid, header.getSeq(), REGISTER_RSP));
        RegisterRsp.Builder builder1 = RegisterRsp.newBuilder();
        builder1.setResponse(Protobuf.setResponseMessage(success, (success ? "" : "密码错误")));
        builder.setRegisterRsp(builder1.build());

        return builder.build();
    }

    // 处理心跳消息
    public PDGMessage processHeartbeat(PDGMessage heartbeat) {
        PDGHeader header = heartbeat.getPDGHeader();

        log.info("心跳: " + header.getFromAddr() + '/' + header.getStationCode());
        PDGMessage.Builder builder = PDGMessage.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), header.getSession(), header.getSeq(), HEARTBEAT_RSP));

        return builder.build();
    }

    // 处理目录推送消息
    public PDGMessage processCatalog(PDGMessage message) {
        PDGHeader header = message.getPDGHeader();
        log.info("上报设备列表: " + header.getFromAddr() + '/' + header.getStationCode());

        MessageProtobuf.Catalog catalog = message.getCatalog();
        StationInfo stationInfo = new StationInfo();
        stationInfo.setCode(catalog.getStationCode());
        stationInfo.setName(catalog.getStationName());
        stationInfo.setLongitude(catalog.getLongitude());
        stationInfo.setLatitude(catalog.getLatitude());
        stationInfo.setParentID(0);

        int regionCount = 0;
        int deviceCount = 0;
        List<StationRegion> regionList = new LinkedList<>();
        List<StationDevice> deviceList = new LinkedList<>();
        log.info("站点[" + stationInfo.getCode() + "]共有子设备数: " + catalog.getSensorSubNum());
        if (catalog.getSensorSubNum() > 0) {
            for (MessageProtobuf.SensorDeviceInfo sdi : catalog.getDeviceInfoList()) {
                int deviceType = CodeDeviceType.getDeviceType(sdi.getDeviceCode());
                if (deviceType == CodeDeviceType.REGION || deviceType == CodeDeviceType.PRIMARY_EQUIPMENT) {
                    // 处理区域
                    StationRegion region = new StationRegion();
                    region.setCode(sdi.getDeviceCode());
                    region.setName(sdi.getDeviceName());
                    region.setParentCode(sdi.getParentCode());
                    region.setStationCode(stationInfo.getCode());
                    regionList.add(region);
                    regionCount++;
                } else if (deviceType == CodeDeviceType.VIDEO) {
                    // 处理摄像机


                } else {
                    // 处理设备
                    StationDevice device = new StationDevice();
                    device.setCode(sdi.getDeviceCode());
                    device.setName(sdi.getDeviceName());
                    device.setStationCode(stationInfo.getCode());
                    if (sdi.getParentCode().equals(device.getStationCode())) {
                        device.setRegionCode(null);
                        device.setRegionID(0);
                    } else {
                        device.setRegionCode(sdi.getParentCode());
                    }
                    deviceList.add(device);
                    deviceCount++;
                }
            }
        }

        log.info("站点[" + stationInfo.getCode() + "]共有区域数: " + regionCount + "，设备数: " + deviceCount);

        // 写目录信息到数据库
        ClientManager.instance().processCatalog(stationInfo);

        // 写区域信息到数据库
        ClientManager.instance().processRegion(regionList);

        // 写设备信息到数据库
        ClientManager.instance().processDevice(deviceList);

        // 写摄像机信息到数据库

        // 回包
        PDGMessage.Builder builder = PDGMessage.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), header.getSession(), header.getSeq(), CATALOG_RSP));
        CatalogRsp.Builder builder1 = CatalogRsp.newBuilder();
        builder1.setResponse(Protobuf.setResponseMessage(true, null));
        builder.setCatalogRsp(builder1.build());
        return builder.build();
    }

    public PDGMessage processReportSensorData(PDGMessage sensorData) {
        MessageProtobuf.PDGHeader header = sensorData.getPDGHeader();

        PDGMessage.Builder builder = PDGMessage.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), header.getSession(), header.getSeq(), REPORT_ENVDATA_RSP));
        ReportSensorDataRsp.Builder builder1 = ReportSensorDataRsp.newBuilder();
        builder1.setResponse(Protobuf.setResponseMessage(true, null));
        builder.setReportSensorDataRsp(builder1.build());

        SampleSingal s = new SampleSingal();
        s.setValue(sensorData.getReportSensorData().getValue());
        s.setCode(sensorData.getReportSensorData().getDeviceCode());
        ClientManager.instance().processSampleSingal(s);

        return builder.build();
    }

    public PDGMessage processAlarm(PDGMessage alarm) {
        MessageProtobuf.PDGHeader header = alarm.getPDGHeader();

        PDGMessage.Builder builder = PDGMessage.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), header.getSession(), header.getSeq(), REPORT_ALARM_RSP));
        ReportAlarmRsp.Builder builder1 = ReportAlarmRsp.newBuilder();
        builder1.setResponse(Protobuf.setResponseMessage(true, null));
        builder.setReportAlarmRsp(builder1.build());

        Event e = new Event();
        ReportAlarm reportAlarm = alarm.getReportAlarm();
        e.setCode(reportAlarm.getDeviceCode());
        e.setEventState(reportAlarm.getAlarmStateValue());
        e.setEventType(reportAlarm.getAlarmTypeValue());
        e.setEventValue(reportAlarm.getValue());
        ClientManager.instance().processEvent(e);

        return builder.build();
    }

    public PDGMessage processUpdateTime(PDGMessage updateTime) {
        MessageProtobuf.PDGHeader header = updateTime.getPDGHeader();

        PDGMessage.Builder builder = PDGMessage.newBuilder();
        UpdateTimeRsp.Builder builder1 = UpdateTimeRsp.newBuilder();
        builder.setPDGHeader(Protobuf.setPDGHeader(header.getStationCode(), header.getSession(), header.getSeq(), SYSTEM_TIME_RSP));
        builder1.setServerTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        builder.setUpdateTimeRsp(builder1.build());

        return builder.build();
    }


}
