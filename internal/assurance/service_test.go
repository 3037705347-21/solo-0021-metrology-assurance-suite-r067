package assurance_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"example.com/solo-0021-metrology-assurance-suite/internal/assurance"
	"example.com/solo-0021-metrology-assurance-suite/internal/contracts"
	"example.com/solo-0021-metrology-assurance-suite/internal/domain"
	"example.com/solo-0021-metrology-assurance-suite/internal/repository"
)

func newService() *assurance.Service {
	return assurance.NewService(repository.New())
}

func register(t *testing.T, s *assurance.Service, tag string) domain.Device {
	t.Helper()
	device, err := s.RegisterDevice(context.Background(), contracts.RegisterDeviceRequest{
		AssetTag: tag,
		Model:    "Bench Meter",
		Room:     "Room A",
		Steward:  "Lin",
	})
	if err != nil {
		t.Fatalf("register %s: %v", tag, err)
	}
	return device
}

func openCase(t *testing.T, s *assurance.Service, deviceID string) contracts.CaseResponse {
	t.Helper()
	view, err := s.OpenCase(context.Background(), contracts.OpenCaseRequest{
		DeviceID:  deviceID,
		Nominal:   25,
		Tolerance: 0.5,
		Steward:   "Qiao",
	})
	if err != nil {
		t.Fatalf("open case for %s: %v", deviceID, err)
	}
	return view
}

func TestConcurrentRegistrationAllowsOneActiveDevicePerTag(t *testing.T) {
	s := newService()

	const contenders = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	results := make([]error, contenders)
	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, results[i] = s.RegisterDevice(context.Background(), contracts.RegisterDeviceRequest{
				AssetTag: " met-2001 ",
				Model:    "Bench Meter",
				Room:     "Room A",
				Steward:  "Lin",
			})
		}(i)
	}
	close(start)
	wg.Wait()

	successes, duplicates := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, domain.ErrDuplicateAssetTag):
			duplicates++
		default:
			t.Fatalf("unexpected registration error: %v", err)
		}
	}
	if successes != 1 || duplicates != contenders-1 {
		t.Fatalf("concurrent registrations: %d success, %d duplicate", successes, duplicates)
	}
}

func TestFailedRegistrationLeavesNoReservation(t *testing.T) {
	s := newService()

	// A request that fails domain validation must not reserve the tag or burn
	// an id; the exact same tag must register cleanly right afterwards.
	_, err := s.RegisterDevice(context.Background(), contracts.RegisterDeviceRequest{
		AssetTag: "MET-2101",
		Model:    "",
		Room:     "Room A",
		Steward:  "Lin",
	})
	if !errors.Is(err, domain.ErrInvalidInput) {
		t.Fatalf("expected invalid input, got %v", err)
	}
	device := register(t, s, " met-2101 ")
	if device.Enrollment != 1 {
		t.Fatalf("failed attempt advanced enrollment to %d", device.Enrollment)
	}
	if device.ID != "device-0001" {
		t.Fatalf("failed attempt consumed an id, got %q", device.ID)
	}
}

func TestRetireAndReRegisterRacingProduceOneActiveUnit(t *testing.T) {
	s := newService()

	const rounds = 64
	for round := range rounds {
		tag := fmt.Sprintf("MET-22%02d", round)
		first := register(t, s, tag)
		// Give the predecessor a finished case so retirement is legal.
		view := openCase(t, s, first.ID)
		if _, err := s.RecordMeasurement(context.Background(), view.Case.ID, contracts.RecordMeasurementRequest{
			Observed:   25.2,
			Technician: "Rui",
		}); err != nil {
			t.Fatalf("record measurement: %v", err)
		}
		if _, err := s.SealCase(context.Background(), view.Case.ID, contracts.SealCaseRequest{
			Reviewer: "Verifier",
			Summary:  "done",
		}); err != nil {
			t.Fatalf("seal predecessor case: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		var retireErr, registerErr error
		var successor domain.Device
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, retireErr = s.RetireDevice(context.Background(), first.ID, contracts.RetireDeviceRequest{
				Reason: "decommissioned",
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			successor, registerErr = s.RegisterDevice(context.Background(), contracts.RegisterDeviceRequest{
				AssetTag: tag,
				Model:    "Bench Meter",
				Room:     "Room A",
				Steward:  "Lin",
			})
		}()
		close(start)
		wg.Wait()

		if retireErr != nil {
			t.Fatalf("round %d: retirement failed: %v", round, retireErr)
		}
		// If the commit order retired the predecessor after registration lost
		// the race, registration reports a duplicate; otherwise it succeeds.
		// Either way at most one unit may be active for the tag.
		switch {
		case registerErr == nil:
			if successor.ReplacedDeviceID != first.ID || successor.Enrollment != 2 {
				t.Fatalf("round %d: bad successor %+v", round, successor)
			}
		case errors.Is(registerErr, domain.ErrDuplicateAssetTag):
			// The tag is still held by the (as-yet-unretired) predecessor; a
			// strictly serialized retry now succeeds.
			successor = register(t, s, tag)
			if successor.ReplacedDeviceID != first.ID {
				t.Fatalf("round %d: successor does not replace predecessor", round)
			}
		default:
			t.Fatalf("round %d: unexpected registration error: %v", round, registerErr)
		}

		firstView, err := s.Device(context.Background(), first.ID)
		if err != nil {
			t.Fatalf("round %d: read predecessor: %v", round, err)
		}
		successorView, err := s.Device(context.Background(), successor.ID)
		if err != nil {
			t.Fatalf("round %d: read successor: %v", round, err)
		}
		if firstView.IsActive() || !successorView.IsActive() {
			t.Fatalf("round %d: predecessor active=%v successor active=%v", round, firstView.IsActive(), successorView.IsActive())
		}
	}
}

func TestRetirementBlockedWhileCaseUnfinishedAndCaseStillCompletable(t *testing.T) {
	s := newService()
	device := register(t, s, "MET-2301")
	view := openCase(t, s, device.ID)

	_, err := s.RetireDevice(context.Background(), device.ID, contracts.RetireDeviceRequest{
		Reason: "too early",
	})
	if !errors.Is(err, domain.ErrActiveCaseExists) {
		t.Fatalf("retire with open case: expected active case conflict, got %v", err)
	}

	current, err := s.Device(context.Background(), device.ID)
	if err != nil {
		t.Fatalf("read device: %v", err)
	}
	if !current.IsActive() {
		t.Fatal("device became retired despite the rejected request")
	}

	// The case keeps every normal route until it is sealed.
	measured, err := s.RecordMeasurement(context.Background(), view.Case.ID, contracts.RecordMeasurementRequest{
		Observed:   26,
		Technician: "Rui",
	})
	if err != nil {
		t.Fatalf("record measurement: %v", err)
	}
	if measured.Case.State != domain.CaseRecheckRequired {
		t.Fatalf("state = %s", measured.Case.State)
	}
	reopened, err := s.ReopenCase(context.Background(), view.Case.ID, contracts.ReopenCaseRequest{
		Actor:  "Lin",
		Reason: "fixture replaced",
	})
	if err != nil {
		t.Fatalf("reopen case: %v", err)
	}
	if reopened.Case.State != domain.CaseAwaitingMeasurement {
		t.Fatalf("state = %s", reopened.Case.State)
	}
	if _, err := s.RecordMeasurement(context.Background(), view.Case.ID, contracts.RecordMeasurementRequest{
		Observed:   25.1,
		Technician: "Rui",
	}); err != nil {
		t.Fatalf("record accepted measurement: %v", err)
	}
	sealed, err := s.SealCase(context.Background(), view.Case.ID, contracts.SealCaseRequest{
		Reviewer: "Verifier",
		Summary:  "within tolerance",
	})
	if err != nil {
		t.Fatalf("seal case: %v", err)
	}
	if sealed.Case.State != domain.CaseSealed {
		t.Fatalf("state = %s", sealed.Case.State)
	}

	// Only once the case is sealed may the unit retire.
	retired, err := s.RetireDevice(context.Background(), device.ID, contracts.RetireDeviceRequest{
		Reason: "replaced by new unit",
	})
	if err != nil {
		t.Fatalf("retire after seal: %v", err)
	}
	if retired.IsActive() {
		t.Fatal("device still active after retirement")
	}

	// The finished case and its evidence stay readable on the retired unit.
	historical, err := s.Case(context.Background(), view.Case.ID)
	if err != nil {
		t.Fatalf("read historical case: %v", err)
	}
	if historical.Seal == nil || historical.Seal.DeviceID != device.ID {
		t.Fatalf("seal attribution = %+v, want device %s", historical.Seal, device.ID)
	}
}

func TestSuccessorDeviceOpensIndependentCase(t *testing.T) {
	s := newService()
	first := register(t, s, "MET-2401")
	oldCase := openCase(t, s, first.ID)

	// The predecessor cannot retire with an open case; finish it to free the
	// tag, then enroll a successor under the same asset number.
	if _, err := s.RecordMeasurement(context.Background(), oldCase.Case.ID, contracts.RecordMeasurementRequest{
		Observed:   25.2,
		Technician: "Rui",
	}); err != nil {
		t.Fatalf("record measurement: %v", err)
	}
	if _, err := s.SealCase(context.Background(), oldCase.Case.ID, contracts.SealCaseRequest{
		Reviewer: "Verifier",
		Summary:  "done",
	}); err != nil {
		t.Fatalf("seal old case: %v", err)
	}
	if _, err := s.RetireDevice(context.Background(), first.ID, contracts.RetireDeviceRequest{
		Reason: "cycle complete",
	}); err != nil {
		t.Fatalf("retire predecessor: %v", err)
	}

	successor := register(t, s, "met-2401")
	if successor.ID == first.ID || successor.Enrollment != 2 {
		t.Fatalf("successor identity = %s enrollment %d", successor.ID, successor.Enrollment)
	}

	// The new unit must be able to open its own case: it does not inherit the
	// predecessor's finished case.
	newView, err := s.OpenCase(context.Background(), contracts.OpenCaseRequest{
		DeviceID:  successor.ID,
		Nominal:   25,
		Tolerance: 0.5,
		Steward:   "Qiao",
	})
	if err != nil {
		t.Fatalf("successor open case: %v", err)
	}
	if newView.Case.DeviceID != successor.ID {
		t.Fatalf("new case bound to %s", newView.Case.DeviceID)
	}

	// A second open case on the successor is still rejected independently.
	_, err = s.OpenCase(context.Background(), contracts.OpenCaseRequest{
		DeviceID:  successor.ID,
		Nominal:   25,
		Tolerance: 0.5,
		Steward:   "Qiao",
	})
	if !errors.Is(err, domain.ErrActiveCaseExists) {
		t.Fatalf("second successor case: expected conflict, got %v", err)
	}

	// The predecessor's sealed evidence still names the predecessor.
	oldView, err := s.Case(context.Background(), oldCase.Case.ID)
	if err != nil {
		t.Fatalf("read predecessor case: %v", err)
	}
	if oldView.Seal == nil || oldView.Seal.DeviceID != first.ID {
		t.Fatalf("predecessor seal reassigned: %+v", oldView.Seal)
	}
}
