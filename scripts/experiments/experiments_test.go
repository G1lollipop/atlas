package main

import (
	"encoding/csv"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExperimentSchemaNameIsSearchPathSafe(t *testing.T) {
	got := experimentSchemaName("20260930T153045Z-a1b2c3d4")
	if got != strings.ToLower(got) || strings.Contains(got, "-") {
		t.Fatalf("schema name %q must be lowercase and dash-free", got)
	}
	if !strings.HasPrefix(got, "atlas_exp_") {
		t.Fatalf("schema name %q has unexpected prefix", got)
	}
	if err := validateExperimentSchemaOwnership(got, got); err != nil {
		t.Fatalf("generated schema failed ownership guard: %v", err)
	}
	for _, test := range []struct{ current, expected string }{
		{current: "atlas_exp_other", expected: got},
		{current: "public", expected: "public"},
		{current: "atlas_exp_bad", expected: "ATLAS_EXP_BAD"},
	} {
		if err := validateExperimentSchemaOwnership(test.current, test.expected); err == nil {
			t.Errorf("schema guard accepted current=%q expected=%q", test.current, test.expected)
		}
	}
}

func TestPercentileSortsUnorderedObservations(t *testing.T) {
	got := percentile([]float64{9, 1, 5}, .95)
	if got != 9 {
		t.Fatalf("p95 = %v, want 9", got)
	}
}

func TestProcStatParsingHandlesParenthesesInProcessName(t *testing.T) {
	fields := "R 1 0 0 0 0 0 0 0 0 0 17 4 0 0 0 0 0 0 12345"
	entry, err := parseProcEntry(42, "42 (postgres (writer)) "+fields)
	if err != nil {
		t.Fatal(err)
	}
	if entry.pid != 42 || entry.ppid != 1 || entry.userTicks != 17 || entry.systemTicks != 4 || entry.startTime != 12345 {
		t.Fatalf("parsed proc stat = %+v", entry)
	}
}

func TestReportCSVsKeepReservationMetricsInRows(t *testing.T) {
	out := t.TempDir()
	reserved := 4096.0
	stranded := 2048.0
	trial := trialResult{
		ScenarioID: "trial", WorkloadProfile: "mixed", Policy: "best-fit", WorkerCount: 2,
		MeanGPUReservedVRAMMB: &reserved, MeanWholeDeviceStrandedVRAMMB: &stranded,
		SmallGPUJobsAccepted: 3, SmallGPUJobsPlacedOnLargeGPU: 1,
	}
	if err := writeTrialsCSV(filepath.Join(out, "trials.csv"), []trialResult{trial}); err != nil {
		t.Fatal(err)
	}
	sample := dbSample{At: time.Now().UTC(), ScenarioID: "trial", GPUAvailableDevices: 1,
		GPUReservedDevices: 1, WholeDeviceStrandedVRAMMB: 2048, LargeJobSlackMB: 8192}
	if err := writeSamplesCSV(filepath.Join(out, "samples.csv"), []dbSample{sample}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"trials.csv", "samples.csv"} {
		f, err := os.Open(filepath.Join(out, name))
		if err != nil {
			t.Fatal(err)
		}
		rows, err := csv.NewReader(f).ReadAll()
		closeErr := f.Close()
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if closeErr != nil {
			t.Fatalf("close %s: %v", name, closeErr)
		}
		if len(rows) != 2 || len(rows[0]) != len(rows[1]) {
			t.Fatalf("%s header/row columns = %d/%d", name, len(rows[0]), len(rows[1]))
		}
		joined := strings.Join(rows[0], ",")
		if !strings.Contains(joined, "whole_device_stranded_vram_mb") {
			t.Errorf("%s header omits whole-device stranded VRAM metric: %s", name, joined)
		}
	}
}

func TestTCPProxyDisableDropsExistingAndNewConnections(t *testing.T) {
	backend, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	backendDone := make(chan struct{})
	defer func() {
		if err := backend.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Errorf("close test backend: %v", err)
		}
		<-backendDone
	}()
	go func() {
		defer close(backendDone)
		for {
			conn, err := backend.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() {
					if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
						t.Errorf("close echo connection: %v", err)
					}
				}()
				if _, err := io.Copy(conn, conn); err != nil {
					return
				}
			}()
		}
	}()

	proxy, err := newTCPProxy(backend.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer proxy.Close()

	client, err := net.DialTimeout("tcp", proxy.Addr(), time.Second)
	if err != nil {
		t.Fatalf("connect through available proxy: %v", err)
	}
	defer closeTestConn(t, client, "initial client")
	if err := client.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Write([]byte("x")); err != nil {
		t.Fatalf("write through available proxy: %v", err)
	}
	var echoed [1]byte
	if _, err := io.ReadFull(client, echoed[:]); err != nil || echoed[0] != 'x' {
		t.Fatalf("initial proxied echo = %q, %v", echoed[:], err)
	}

	proxy.SetAvailable(false)
	if _, err := client.Read(echoed[:]); err == nil {
		t.Fatal("existing client remained connected after proxy isolation")
	}
	blocked, err := net.DialTimeout("tcp", proxy.Addr(), time.Second)
	if err == nil {
		defer closeTestConn(t, blocked, "isolated client")
		if err := blocked.SetDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := blocked.Write([]byte("x")); err == nil {
			if _, err := blocked.Read(echoed[:]); err == nil {
				t.Fatal("new client received a proxied response while isolated")
			}
		}
	}

	proxy.SetAvailable(true)
	restored, err := net.DialTimeout("tcp", proxy.Addr(), time.Second)
	if err != nil {
		t.Fatalf("connect after restoring proxy: %v", err)
	}
	defer closeTestConn(t, restored, "restored client")
	if err := restored.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.Write([]byte("y")); err != nil {
		t.Fatalf("write after restoring proxy: %v", err)
	}
	if _, err := io.ReadFull(restored, echoed[:]); err != nil || echoed[0] != 'y' {
		t.Fatalf("restored proxied echo = %q, %v", echoed[:], err)
	}
}

func closeTestConn(t *testing.T, conn net.Conn, label string) {
	t.Helper()
	if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("close %s: %v", label, err)
	}
}
