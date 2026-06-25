package dup

import "testing"

// TestResolveListenAddr pins how the optional CLI argument maps to a bind
// address: a bare port stays loopback, while an explicit host:port (incl.
// 0.0.0.0 / a LAN IP / IPv6 / ":port") binds as given and is flagged "exposed".
func TestResolveListenAddr(t *testing.T) {
	t.Setenv("DUPCLEANER_HOST", "") // isolate from the host env override

	cases := []struct {
		arg     string
		addr    string
		browse  string
		exposed bool
	}{
		{"", "127.0.0.1:7891", "http://127.0.0.1:7891", false},
		{"8030", "127.0.0.1:8030", "http://127.0.0.1:8030", false},
		{"0.0.0.0:8030", "0.0.0.0:8030", "http://127.0.0.1:8030", true}, // open loopback, bind all
		{"192.168.1.5:8030", "192.168.1.5:8030", "http://192.168.1.5:8030", true},
		{":8030", "0.0.0.0:8030", "http://127.0.0.1:8030", true},
		{"localhost:8030", "localhost:8030", "http://localhost:8030", false},
		{"[::1]:8030", "[::1]:8030", "http://[::1]:8030", false},
	}
	for _, c := range cases {
		addr, browse, exposed := resolveListenAddr(c.arg)
		if addr != c.addr || browse != c.browse || exposed != c.exposed {
			t.Errorf("resolveListenAddr(%q) = (%q, %q, %v), want (%q, %q, %v)",
				c.arg, addr, browse, exposed, c.addr, c.browse, c.exposed)
		}
	}
}

// TestResolveListenAddr_HostEnv verifies DUPCLEANER_HOST still applies when only
// a bare port is passed, but an explicit host:port arg overrides it.
func TestResolveListenAddr_HostEnv(t *testing.T) {
	t.Setenv("DUPCLEANER_HOST", "0.0.0.0")
	if addr, _, exposed := resolveListenAddr("8030"); addr != "0.0.0.0:8030" || !exposed {
		t.Errorf("bare port with DUPCLEANER_HOST=0.0.0.0: got %q exposed=%v, want 0.0.0.0:8030 exposed=true", addr, exposed)
	}
	if addr, _, _ := resolveListenAddr("127.0.0.1:8030"); addr != "127.0.0.1:8030" {
		t.Errorf("explicit host:port should override DUPCLEANER_HOST: got %q", addr)
	}
}
