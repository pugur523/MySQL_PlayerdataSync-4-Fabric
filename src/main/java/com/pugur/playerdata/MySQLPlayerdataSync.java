package com.pugur.playerdata;

import com.mojang.brigadier.StringReader;
import com.mojang.brigadier.exceptions.CommandSyntaxException;
import com.pugur.playerdata.config.ConfigManager;
import net.fabricmc.api.ModInitializer;
import net.minecraft.entity.player.PlayerInventory;
import net.minecraft.inventory.EnderChestInventory;
import net.minecraft.nbt.NbtElement;
import net.minecraft.nbt.NbtList;
import net.minecraft.nbt.StringNbtReader;
import net.minecraft.network.packet.s2c.play.UpdateSelectedSlotS2CPacket;
import net.minecraft.server.dedicated.ServerPropertiesHandler;
import net.minecraft.server.dedicated.ServerPropertiesLoader;
import net.minecraft.server.network.ServerPlayerEntity;
import net.minecraft.text.Text;
import net.minecraft.util.Formatting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MySQLPlayerdataSync implements ModInitializer {

    public static final Logger logger = LoggerFactory.getLogger("Fabric Playerdata Sync");
    private static String SERVER_NAME;
    private static String JDBC_URL;
    private static String USER;
    private static String PASSWORD;
    public static boolean is_enabled = true;
    private static final Map<Object, Integer> attempts = new HashMap<>();

    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public static void delayTask(Runnable task, long delay, TimeUnit timeUnit) {
        scheduler.schedule(task, delay, timeUnit);
    }

    @Override
    public void onInitialize() {
        ConfigManager.init();
        JDBC_URL = ConfigManager.properties.getProperty("jdbc.url");
        USER = ConfigManager.properties.getProperty("jdbc.user");
        PASSWORD = ConfigManager.properties.getProperty("jdbc.password");

        if (JDBC_URL.isEmpty() || USER.isEmpty() || PASSWORD.isEmpty()) {
            logger.info("Configファイルにデータベースの設定が見つかりませんでした。Fabric - MySQL Player Data Syncを無効化します。");
            is_enabled = false;
            return;
        }

        createTable();
        getLevelName();
        logger.info("server name is : " + SERVER_NAME);
        logger.info("Fabric Playerdata Sync has initialized 🔥");
    }

    public static void savePlayerDataToDatabase(ServerPlayerEntity player) {
        if (is_enabled) {
            try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
                boolean crash_flag = false;
                String check = "SELECT crash_flag FROM playerdata WHERE uuid = ?";
                try (PreparedStatement s1 = connection.prepareStatement(check)) {
                    s1.setString(1, player.getUuidAsString());
                    try (ResultSet resultSet = s1.executeQuery()) {
                        if (resultSet.next()) {
                            crash_flag = resultSet.getBoolean("crash_flag");
                        }
                    }
                }

                if (!crash_flag) {
                    String saving = "UPDATE playerdata SET saving_flag = ? WHERE uuid = ?";
                    try (PreparedStatement s = connection.prepareStatement(saving)) {
                        s.setBoolean(1, true);
                        s.setString(2, player.getUuidAsString());
                        s.executeUpdate();
                    }

                    String sql = "INSERT INTO playerdata (uuid, inventory, enderitems, selecteditemslot, conn_flag, saving_flag) " +
                            "VALUES (?, ?, ?, ?, ?, ?) " +
                            "ON DUPLICATE KEY UPDATE " +
                            "inventory = VALUES(inventory), enderitems = VALUES(enderitems), selecteditemslot = VALUES(selecteditemslot), conn_flag = VALUES(conn_flag), saving_flag = VALUES(saving_flag)";
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, player.getUuidAsString());
                        statement.setString(2, getPlayerInventory(player));
                        statement.setString(3, getPlayerEnderitems(player));
                        statement.setInt(4, player.getInventory().selectedSlot);
                        statement.setBoolean(5, false);
                        statement.setBoolean(6, false);
                        statement.executeUpdate();
                        logger.info("🔥 " + player.getName().getString() + " saved playerdata to mysql");
                    }
                }
            } catch (SQLException | IOException e) {
                logger.error("Error while saving player data to mysql: " + e.getMessage());
            }
        }
    }

    public static void loadPlayerDataFromDatabase(ServerPlayerEntity player) {
        if (is_enabled) {
            final Integer counter = attempts.getOrDefault(player.getUuidAsString(), 0);
            attempts.put(player.getUuidAsString(), counter + 1);
            try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
                String sql = "SELECT inventory, enderitems, selecteditemslot, conn_flag, saving_flag, rollback_flag, crash_flag, lastserver, rollbackserver FROM playerdata WHERE uuid = ?";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, player.getUuidAsString());

                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            boolean conn_flag = resultSet.getBoolean("conn_flag");
                            boolean saving_flag = resultSet.getBoolean("saving_flag");
                            boolean rollback_flag = resultSet.getBoolean("rollback_flag");
                            boolean crash_flag = resultSet.getBoolean("crash_flag");
                            String lastServer = resultSet.getString("lastserver");
                            String rollbackServer = resultSet.getString("rollbackserver");

                            if (crash_flag) {
                                if (lastServer.equals(SERVER_NAME)) {
                                    crashRestore(player, lastServer);
                                } else {
                                    crashSkip(player, lastServer);
                                }
                            } else if (rollback_flag) {
                                if (rollbackServer.equals(SERVER_NAME)) {
                                    rollbackRestore(player, rollbackServer);
                                } else {
                                    rollbackSkip(player, rollbackServer);
                                }
                            } else if (conn_flag) {
                                logger.info("conn_flag is true");
                                    if (counter < 100) {
                                        delayTask(() -> loadPlayerDataFromDatabase(player), 50, TimeUnit.MILLISECONDS);
                                    } else {
                                        if (saving_flag) {
                                            logger.info("saving_flag is true");
                                            player.sendMessage(
                                                    Text.literal("Fabric プレイヤーデータ共有 : ").formatted(Formatting.LIGHT_PURPLE).append(
                                                            Text.literal("失敗").formatted(Formatting.DARK_RED)
                                                    )
                                            );
                                            player.sendMessage(
                                                    Text.literal(lastServer).formatted(Formatting.GREEN).append(
                                                            Text.literal("に入りなおしてください").formatted(Formatting.WHITE)
                                                    )
                                            );
                                            String crashSet = "UPDATE playerdata SET crash_flag = true WHERE uuid = ?";
                                            try (PreparedStatement s = connection.prepareStatement(crashSet)) {
                                                s.setString(1, player.getUuidAsString());
                                                s.executeUpdate();
                                            }
                                        } else {
                                            if (lastServer.equals(SERVER_NAME)) {
                                                crashRestore(player, lastServer);
                                            } else {
                                                crashSkip(player, lastServer);
                                            }
                                        }
                                        attempts.put(player.getUuidAsString(), 0);
                                    }
                            } else {
                                setPlayerInventory(player.getInventory(), resultSet.getString("inventory"));
                                setPlayerEnderitems(player.getEnderChestInventory(), resultSet.getString("enderitems"));
                                player.getInventory().selectedSlot = resultSet.getInt("selecteditemslot");
                                player.networkHandler.sendPacket(new UpdateSelectedSlotS2CPacket(resultSet.getInt("selecteditemslot")));

                                String connSet = "UPDATE playerdata SET conn_flag = ?, lastserver = ? WHERE uuid = ?";
                                try (PreparedStatement s = connection.prepareStatement(connSet)) {
                                    s.setBoolean(1, true);
                                    s.setString(2, SERVER_NAME);
                                    s.setString(3, player.getUuidAsString());
                                    s.executeUpdate();
                                }

                                logger.info("🔥 " + player.getName().getString() + " loaded playerdata from mysql");
                                player.sendMessage(
                                        Text.literal("Fabric プレイヤーデータ共有 : ").formatted(Formatting.LIGHT_PURPLE).append(
                                                Text.literal("完了").formatted(Formatting.AQUA)
                                        )
                                );
                            }
                        } else {
                            String registerPlayer = "INSERT INTO playerdata (uuid, conn_flag, lastserver) VALUES (?, ?, ?)";
                            try (PreparedStatement s2 = connection.prepareStatement(registerPlayer)) {
                                s2.setString(1, player.getUuidAsString());
                                s2.setBoolean(2, true);
                                s2.setString(3, SERVER_NAME);
                                s2.executeUpdate();
                            }
                            logger.info("🔥 " + player.getName().getString() + " was registered to mysql");
                        }
                    }
                }
            } catch (SQLException | CommandSyntaxException e) {
                logger.error("Error while loading player data from database: " + e.getMessage());
                player.sendMessage(
                        Text.literal("Fabric プレイヤーデータ共有 : ").formatted(Formatting.YELLOW).append(
                                Text.literal("失敗(SQL Exception | CommandSyntaxException)").formatted(Formatting.DARK_RED)
                        )
                );
            }
        } else {
            player.sendMessage(
                    Text.literal("Fabric プレイヤーデータ共有 : ").formatted(Formatting.YELLOW).append(
                            Text.literal("無効(Config Error)").formatted(Formatting.DARK_RED)
                    )
            );
        }
    }

    public static String getPlayerInventory(ServerPlayerEntity player) throws IOException {
        PlayerInventory playerInventory = player.getInventory();
        NbtList inventoryList = new NbtList();
        NbtList nbtInventory = playerInventory.writeNbt(inventoryList);
        return nbtInventory.toString();
    }

    public static String getPlayerEnderitems(ServerPlayerEntity player) {
        EnderChestInventory enderChestInventory = player.getEnderChestInventory();
        NbtList nbtEnderitems = enderChestInventory.toNbtList();
        return nbtEnderitems.toString();
    }

    public static void setPlayerInventory(PlayerInventory playerInventory, String inventoryString) throws CommandSyntaxException {
        playerInventory.clear();
        NbtList inventoryList = parseFromString(inventoryString);
        playerInventory.readNbt(inventoryList);
    }

    public static void setPlayerEnderitems(EnderChestInventory enderChestInventory, String enderItemsString) throws CommandSyntaxException {
        enderChestInventory.clear();
        NbtList enderItemsList = parseFromString(enderItemsString);
        enderChestInventory.readNbtList(enderItemsList);
    }

    /*
    public static NbtList parseFromString(String nbtString) throws CommandSyntaxException {
        StringReader reader = new StringReader(nbtString);
        StringNbtReader nbtReader = new StringNbtReader(reader);
        NbtElement element = nbtReader.parseElement();
        if (element instanceof NbtList) {
            return (NbtList) element;
        }
        return new NbtList();
    }

     */

    public static NbtList parseFromString(String nbtString) throws CommandSyntaxException {
        StringReader reader = new StringReader(nbtString);
        StringNbtReader nbtReader = new StringNbtReader(reader);
        NbtElement element = nbtReader.parseElement();
        if (!(element instanceof NbtList)) {
            throw new IllegalArgumentException("Parsed NbtElement is not of type NbtList");
        }
        return (NbtList) element;
    }

    public static void crashSkip(ServerPlayerEntity player, String lastServer) {
        player.sendMessage(
                Text.literal("前回インベントリのセーブに失敗しています。\n").formatted(Formatting.LIGHT_PURPLE).append(
                        Text.literal(lastServer + "に入り直してください").formatted(Formatting.RED)
                ));
        logger.info(player.getName().getString() + " skipped loading playerdata");
    }

    public static void crashRestore(ServerPlayerEntity player, String lastServer) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            player.sendMessage(
                    Text.literal("最後に" + lastServer + "に保存されたインベントリを復元しました").formatted(Formatting.DARK_GREEN)
            );
            String sql = "UPDATE playerdata SET crash_flag = false, saving_flag = false, conn_flag = true WHERE uuid = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, player.getUuidAsString());
                statement.executeUpdate();
                logger.info(player.getName().getString() + " restored playerdata(crash)");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void rollbackSkip(ServerPlayerEntity player, String rollbackServer) {
        player.getInventory().clear();
        player.getEnderChestInventory().clear();
        player.sendMessage(
                Text.literal("サバイバルワールドのロールバックを行いました。\n").formatted(Formatting.LIGHT_PURPLE).append(
                        Text.literal(rollbackServer + "に入り直してください").formatted(Formatting.RED)
                ));
        logger.info(player.getName().getString() + " skipped to load playerdata(rollback)");
    }

    public static void rollbackRestore(ServerPlayerEntity player, String rollbackServer) {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            player.sendMessage(
                    Text.literal("バックアップ時に" + rollbackServer + "に保存されたインベントリを復元しました").formatted(Formatting.DARK_GREEN)
            );
            String sql = "UPDATE playerdata SET rollback_flag = false WHERE uuid = ?";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, player.getUuidAsString());
                statement.executeUpdate();
                logger.info(player.getName().getString() + " restored playerdata(rollback)");
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void createTable() {
        try (Connection connection = DriverManager.getConnection(JDBC_URL, USER, PASSWORD)) {
            String sql = "CREATE TABLE IF NOT EXISTS playerdata (" +
                    "uuid VARCHAR(36) PRIMARY KEY, " +
                    "inventory LONGTEXT, " +
                    "enderitems LONGTEXT, " +
                    "selecteditemslot INT, " +
                    "conn_flag BOOLEAN DEFAULT FALSE, " + //サーバーに参加中trueになる
                    "saving_flag BOOLEAN DEFAULT FALSE, " + //save中trueになる
                    "rollback_flag BOOLEAN DEFAULT FALSE, " +
                    "crash_flag BOOLEAN DEFAULT FALSE, " +
                    "lastserver VARCHAR(256), " +
                    "rollbackserver VARCHAR(256) " +
                    ")";
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate(sql);
            }
            logger.info("🔥 MySQL Data Table was Created Successfully");
        } catch (SQLException e) {
            logger.error("Error while creating MySQL playerdata table on database: " + e.getMessage());
        }
    }

    public static void getLevelName(){
        Path path = Paths.get("server.properties");
        ServerPropertiesLoader serverPropertiesLoader = new ServerPropertiesLoader(path);
        ServerPropertiesHandler serverPropertiesHandler = serverPropertiesLoader.getPropertiesHandler();
        SERVER_NAME = serverPropertiesHandler.levelName;
    }
}
