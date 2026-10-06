package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"time"

	"example.com/solo-0021-metrology-assurance-suite/internal/bootstrap"
)

type apiResponse struct {
	Status int
	Body   map[string]any
	Raw    string
}

type harness struct {
	baseURL string
	client  *http.Client
	server  *httptest.Server
}

func main() {
	workflow := flag.String("workflow", "", "workflow id to verify")
	flag.Parse()

	checks := map[string]func() error{
		"register-device":                 checkRegisterDevice,
		"open-verification-case":          checkOpenVerificationCase,
		"record-measurement":              checkRecordMeasurement,
		"reopen-verification-case":        checkReopenVerificationCase,
		"seal-verification":               checkSealVerification,
		"device-retirement":               checkDeviceRetirement,
		"device-registration-concurrency": checkDeviceRegistrationConcurrency,
	}
	run, ok := checks[*workflow]
	if !ok {
		fmt.Fprintf(flag.CommandLine.Output(), "unknown workflow %q\n", *workflow)
		flag.Usage()
		os.Exit(2)
	}
	if err := run(); err != nil {
		fmt.Printf("workflow %s: failed: %v\n", *workflow, err)
		os.Exit(1)
	}
	fmt.Printf("workflow %s: ok\n", *workflow)
}

func checkRegisterDevice() error {
	h := newHarness()
	defer h.close()

	response, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": " met-1001 ",
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return err
	}
	id, err := nestedString(response.Body, "device", "id")
	if err != nil {
		return err
	}
	tag, err := nestedString(response.Body, "device", "asset_tag")
	if err != nil {
		return err
	}
	if tag != "MET-1001" {
		return fmt.Errorf("asset tag was not normalized: %q", tag)
	}

	read, err := h.request(http.MethodGet, "/devices/"+id, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}
	readID, err := nestedString(read.Body, "device", "id")
	if err != nil {
		return err
	}
	if readID != id {
		return fmt.Errorf("read returned device %q, expected %q", readID, id)
	}

	duplicate, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": "MET-1001",
		"model":     "Other Meter",
		"room":      "Room B",
		"steward":   "Mira",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(duplicate, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(duplicate, "duplicate_asset_tag")
}

func checkOpenVerificationCase() error {
	h := newHarness()
	defer h.close()

	deviceID, err := h.createDevice("MET-1101")
	if err != nil {
		return err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return err
	}
	state, err := nestedString(opened.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "awaiting_measurement" {
		return fmt.Errorf("opened case state = %q", state)
	}

	read, err := h.request(http.MethodGet, "/cases/"+caseID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}

	second, err := h.request(http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID,
		"nominal":   25.0,
		"tolerance": 0.5,
		"steward":   "Qiao",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(second, "active_case_exists")
}

func checkRecordMeasurement() error {
	h := newHarness()
	defer h.close()

	deviceID, err := h.createDevice("MET-1201")
	if err != nil {
		return err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return err
	}
	accepted, err := h.recordMeasurement(caseID, 25.2, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(accepted, http.StatusCreated); err != nil {
		return err
	}
	state, err := nestedString(accepted.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "review_ready" {
		return fmt.Errorf("accepted measurement state = %q", state)
	}
	acceptedValue, err := nestedBool(accepted.Body, "measurements", 0, "accepted")
	if err != nil {
		return err
	}
	if !acceptedValue {
		return errors.New("in-tolerance measurement was not accepted")
	}

	second, err := h.recordMeasurement(caseID, 25.1, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(second, "state_conflict"); err != nil {
		return err
	}

	rejectDeviceID, err := h.createDevice("MET-1202")
	if err != nil {
		return err
	}
	rejectCase, err := h.openCase(rejectDeviceID)
	if err != nil {
		return err
	}
	rejectCaseID, err := nestedString(rejectCase.Body, "case", "id")
	if err != nil {
		return err
	}
	rejected, err := h.recordMeasurement(rejectCaseID, 26.5, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(rejected, http.StatusCreated); err != nil {
		return err
	}
	rejectedState, err := nestedString(rejected.Body, "case", "state")
	if err != nil {
		return err
	}
	if rejectedState != "recheck_required" {
		return fmt.Errorf("out-of-tolerance state = %q", rejectedState)
	}
	rejectedValue, err := nestedBool(rejected.Body, "measurements", 0, "accepted")
	if err != nil {
		return err
	}
	if rejectedValue {
		return errors.New("out-of-tolerance measurement was accepted")
	}
	return nil
}

func checkReopenVerificationCase() error {
	h := newHarness()
	defer h.close()

	caseID, err := h.rejectedCase("MET-1301")
	if err != nil {
		return err
	}
	reopened, err := h.request(http.MethodPost, "/cases/"+caseID+"/reopen", map[string]any{
		"actor":  "Lin",
		"reason": "measurement setup was replaced",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(reopened, http.StatusOK); err != nil {
		return err
	}
	state, err := nestedString(reopened.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "awaiting_measurement" {
		return fmt.Errorf("reopened case state = %q", state)
	}
	measurements, err := nestedSlice(reopened.Body, "measurements")
	if err != nil {
		return err
	}
	if len(measurements) != 1 {
		return fmt.Errorf("reopen changed measurement history: %d entries", len(measurements))
	}

	second, err := h.request(http.MethodPost, "/cases/"+caseID+"/reopen", map[string]any{
		"actor":  "Lin",
		"reason": "duplicate request",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(second, http.StatusConflict); err != nil {
		return err
	}
	if err := expectErrorCode(second, "state_conflict"); err != nil {
		return err
	}

	events, err := h.request(http.MethodGet, "/cases/"+caseID+"/events", nil)
	if err != nil {
		return err
	}
	if err := expectStatus(events, http.StatusOK); err != nil {
		return err
	}
	if !hasEvent(events.Body, "case_reopened") {
		return errors.New("reopen audit event is missing")
	}
	return nil
}

func checkSealVerification() error {
	h := newHarness()
	defer h.close()

	caseID, err := h.readyCase("MET-1401")
	if err != nil {
		return err
	}
	type outcome struct {
		response apiResponse
		err      error
	}
	results := make(chan outcome, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			response, err := h.request(http.MethodPost, "/cases/"+caseID+"/seal", map[string]any{
				"reviewer": fmt.Sprintf("Reviewer-%d", index+1),
				"summary":  "accepted measurement sealed for review",
			})
			results <- outcome{response: response, err: err}
		}(index)
	}
	wait.Wait()
	close(results)

	successes := 0
	conflicts := 0
	for item := range results {
		if item.err != nil {
			return item.err
		}
		switch item.response.Status {
		case http.StatusCreated:
			successes++
		case http.StatusConflict:
			conflicts++
		default:
			return fmt.Errorf("unexpected concurrent seal status %d: %s", item.response.Status, item.response.Raw)
		}
	}
	if successes != 1 || conflicts != 1 {
		return fmt.Errorf("concurrent seal outcomes: %d success, %d conflict", successes, conflicts)
	}

	read, err := h.request(http.MethodGet, "/cases/"+caseID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(read, http.StatusOK); err != nil {
		return err
	}
	state, err := nestedString(read.Body, "case", "state")
	if err != nil {
		return err
	}
	if state != "sealed" {
		return fmt.Errorf("sealed case state = %q", state)
	}
	sealID, err := nestedString(read.Body, "seal", "id")
	if err != nil {
		return err
	}
	if sealID == "" {
		return errors.New("evidence seal is missing")
	}
	return nil
}

func checkDeviceRetirement() error {
	h := newHarness()
	defer h.close()

	// An open, out-of-tolerance case is already in flight when the device is
	// retired. Retirement must not take away any path to completing that case.
	deviceID, err := h.createDevice("MET-1501")
	if err != nil {
		return err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return err
	}
	oldCaseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return err
	}
	rejected, err := h.recordMeasurement(oldCaseID, 26.5, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(rejected, http.StatusCreated); err != nil {
		return err
	}

	retired, err := h.request(http.MethodPost, "/devices/"+deviceID+"/retire", map[string]any{
		"reason": "instrument replaced",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(retired, http.StatusOK); err != nil {
		return err
	}
	status, err := nestedString(retired.Body, "device", "status")
	if err != nil {
		return err
	}
	if status != "retired" {
		return fmt.Errorf("retired device status = %q", status)
	}

	// The stranded case can still reopen, measure, and seal on the retired unit.
	reopened, err := h.request(http.MethodPost, "/cases/"+oldCaseID+"/reopen", map[string]any{
		"actor":  "Lin",
		"reason": "new probe installed on the old instrument",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(reopened, http.StatusOK); err != nil {
		return err
	}
	accepted, err := h.recordMeasurement(oldCaseID, 25.2, "Rui")
	if err != nil {
		return err
	}
	if err := expectStatus(accepted, http.StatusCreated); err != nil {
		return err
	}
	sealed, err := h.request(http.MethodPost, "/cases/"+oldCaseID+"/seal", map[string]any{
		"reviewer": "Verifier",
		"summary":  "sealed after retirement",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(sealed, http.StatusCreated); err != nil {
		return err
	}
	sealedDevice, err := nestedString(sealed.Body, "seal", "device_id")
	if err != nil {
		return err
	}
	if sealedDevice != deviceID {
		return fmt.Errorf("seal device %q does not belong to the original device %q", sealedDevice, deviceID)
	}

	// Re-enroll the same asset number as a brand new device identity.
	reenrolled, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": " met-1501 ",
		"model":     "Replacement Meter",
		"room":      "Room C",
		"steward":   "Mira",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(reenrolled, http.StatusCreated); err != nil {
		return err
	}
	newDeviceID, err := nestedString(reenrolled.Body, "device", "id")
	if err != nil {
		return err
	}
	if newDeviceID == deviceID {
		return errors.New("re-enrollment reused the previous device identity")
	}
	enrollment, err := nestedFloat(reenrolled.Body, "device", "enrollment")
	if err != nil {
		return err
	}
	if enrollment != 2 {
		return fmt.Errorf("re-enrollment number = %v, expected 2", enrollment)
	}
	replaced, err := nestedString(reenrolled.Body, "device", "replaced_device_id")
	if err != nil {
		return err
	}
	if replaced != deviceID {
		return fmt.Errorf("replaced_device_id = %q, expected %q", replaced, deviceID)
	}

	// The new device opens its own case: the sealed case of the previous unit
	// must not count as an unfinished case for it.
	newOpened, err := h.openCase(newDeviceID)
	if err != nil {
		return err
	}
	if err := expectStatus(newOpened, http.StatusCreated); err != nil {
		return err
	}
	newCaseID, err := nestedString(newOpened.Body, "case", "id")
	if err != nil {
		return err
	}
	if newCaseID == oldCaseID {
		return errors.New("new device inherited the previous device's case")
	}

	// The old case view is still readable and still belongs to the old unit;
	// the new unit's case must not see the old evidence.
	oldView, err := h.request(http.MethodGet, "/cases/"+oldCaseID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(oldView, http.StatusOK); err != nil {
		return err
	}
	oldViewDevice, err := nestedString(oldView.Body, "case", "device_id")
	if err != nil {
		return err
	}
	if oldViewDevice != deviceID {
		return fmt.Errorf("old case device_id = %q, expected %q", oldViewDevice, deviceID)
	}
	oldViewSeal, err := nestedString(oldView.Body, "seal", "device_id")
	if err != nil {
		return err
	}
	if oldViewSeal != deviceID {
		return fmt.Errorf("old evidence reassigned to %q", oldViewSeal)
	}

	newView, err := h.request(http.MethodGet, "/cases/"+newCaseID, nil)
	if err != nil {
		return err
	}
	if _, exists := newView.Body["seal"]; exists {
		return errors.New("new device can read the previous device's sealed evidence")
	}
	if measurements := nestedSliceOrEmpty(newView.Body, "measurements"); len(measurements) != 0 {
		return fmt.Errorf("new device inherited %d measurement(s) from the previous device", len(measurements))
	}

	// Two active devices must never share the tag: another enrollment conflicts.
	again, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": "MET-1501",
		"model":     "Third Meter",
		"room":      "Room D",
		"steward":   "Asha",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(again, http.StatusConflict); err != nil {
		return err
	}
	return expectErrorCode(again, "duplicate_asset_tag")
}

func checkDeviceRegistrationConcurrency() error {
	// A never-seen tag registered concurrently: at most one enrollment can win.
	if err := raceFreshTag("MET-1601"); err != nil {
		return err
	}

	// Retire and re-enroll the same tag concurrently: the two requests must be
	// serialized so the tag never has two active holders.
	h := newHarness()
	defer h.close()
	deviceID, err := h.createDevice("MET-1602")
	if err != nil {
		return err
	}

	results, err := raceRequests(2, func() (apiResponse, error) {
		return h.request(http.MethodPost, "/devices/"+deviceID+"/retire", map[string]any{
			"reason": "replaced",
		})
	}, func() (apiResponse, error) {
		return h.request(http.MethodPost, "/devices", map[string]any{
			"asset_tag": "MET-1602",
			"model":     "Replacement",
			"room":      "Room C",
			"steward":   "Mira",
		})
	})
	if err != nil {
		return err
	}
	created := 0
	var replacement apiResponse
	for index, item := range results {
		switch {
		case item.Status == http.StatusOK && index == 0:
		case item.Status == http.StatusCreated && index == 1:
			created++
			replacement = item
		case item.Status == http.StatusConflict && index == 1:
			// Re-enrollment won the race and ran first; retry once now that the
			// original unit is retired.
			created++
			retry, err := h.request(http.MethodPost, "/devices", map[string]any{
				"asset_tag": "MET-1602",
				"model":     "Replacement",
				"room":      "Room C",
				"steward":   "Mira",
			})
			if err != nil {
				return err
			}
			if err := expectStatus(retry, http.StatusCreated); err != nil {
				return err
			}
			replacement = retry
		default:
			return fmt.Errorf("unexpected retire/re-enroll status %d: %s", item.Status, item.Raw)
		}
	}
	if created != 1 {
		return fmt.Errorf("re-enrollment produced %d new devices", created)
	}
	newDeviceID, err := nestedString(replacement.Body, "device", "id")
	if err != nil {
		return err
	}
	if newDeviceID == deviceID {
		return errors.New("re-enrollment reused the retired device identity")
	}
	oldView, err := h.request(http.MethodGet, "/devices/"+deviceID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(oldView, http.StatusOK); err != nil {
		return err
	}
	oldStatus, err := nestedString(oldView.Body, "device", "status")
	if err != nil {
		return err
	}
	if oldStatus != "retired" {
		return fmt.Errorf("original device status = %q, expected retired", oldStatus)
	}
	newView, err := h.request(http.MethodGet, "/devices/"+newDeviceID, nil)
	if err != nil {
		return err
	}
	if err := expectStatus(newView, http.StatusOK); err != nil {
		return err
	}
	newStatus, err := nestedString(newView.Body, "device", "status")
	if err != nil {
		return err
	}
	if newStatus != "active" {
		return fmt.Errorf("re-enrolled device status = %q, expected active", newStatus)
	}

	// A failed registration must leave no number occupation: a valid retry of
	// the same tag afterward must still succeed.
	bad, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": "MET-1603",
		"model":     "",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return err
	}
	if err := expectStatus(bad, http.StatusBadRequest); err != nil {
		return err
	}
	good, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": "met-1603",
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return err
	}
	return expectStatus(good, http.StatusCreated)
}

func raceFreshTag(tag string) error {
	h := newHarness()
	defer h.close()

	results, err := raceRequests(4, func() (apiResponse, error) {
		return h.request(http.MethodPost, "/devices", map[string]any{
			"asset_tag": tag,
			"model":     "Bench Meter",
			"room":      "Room A",
			"steward":   "Lin",
		})
	})
	if err != nil {
		return err
	}
	created := 0
	conflicts := 0
	for _, item := range results {
		switch item.Status {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflicts++
		default:
			return fmt.Errorf("unexpected concurrent register status %d: %s", item.Status, item.Raw)
		}
	}
	if created != 1 || conflicts != len(results)-1 {
		return fmt.Errorf("concurrent register outcomes: %d created, %d conflicts", created, conflicts)
	}
	return nil
}

func raceRequests(count int, calls ...func() (apiResponse, error)) ([]apiResponse, error) {
	type outcome struct {
		index    int
		response apiResponse
		err      error
	}
	results := make(chan outcome, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		call := calls[index%len(calls)]
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			response, err := call()
			results <- outcome{index: index, response: response, err: err}
		}(index)
	}
	wait.Wait()
	close(results)

	ordered := make([]apiResponse, count)
	for item := range results {
		if item.err != nil {
			return nil, item.err
		}
		ordered[item.index] = item.response
	}
	return ordered, nil
}

func newHarness() *harness {
	server := httptest.NewServer(bootstrap.NewHandler())
	return &harness{
		baseURL: server.URL,
		client:  server.Client(),
		server:  server,
	}
}

func (h *harness) close() {
	h.server.Close()
}

func (h *harness) request(method, path string, body any) (apiResponse, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return apiResponse{}, err
		}
		reader = bytes.NewReader(encoded)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, method, h.baseURL+path, reader)
	if err != nil {
		return apiResponse{}, err
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := h.client.Do(request)
	if err != nil {
		return apiResponse{}, err
	}
	defer response.Body.Close()
	rawBytes, err := io.ReadAll(response.Body)
	if err != nil {
		return apiResponse{}, err
	}
	result := apiResponse{Status: response.StatusCode, Raw: string(rawBytes)}
	if len(bytes.TrimSpace(rawBytes)) > 0 {
		if err := json.Unmarshal(rawBytes, &result.Body); err != nil {
			return apiResponse{}, fmt.Errorf("decode %s %s response: %w", method, path, err)
		}
	}
	return result, nil
}

func (h *harness) createDevice(tag string) (string, error) {
	response, err := h.request(http.MethodPost, "/devices", map[string]any{
		"asset_tag": tag,
		"model":     "Bench Meter",
		"room":      "Room A",
		"steward":   "Lin",
	})
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return nestedString(response.Body, "device", "id")
}

func (h *harness) openCase(deviceID string) (apiResponse, error) {
	response, err := h.request(http.MethodPost, "/cases", map[string]any{
		"device_id": deviceID,
		"nominal":   25.0,
		"tolerance": 0.5,
		"steward":   "Qiao",
	})
	if err != nil {
		return apiResponse{}, err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return apiResponse{}, err
	}
	return response, nil
}

func (h *harness) recordMeasurement(caseID string, observed float64, technician string) (apiResponse, error) {
	return h.request(http.MethodPost, "/cases/"+caseID+"/measurements", map[string]any{
		"observed":   observed,
		"technician": technician,
	})
}

func (h *harness) rejectedCase(tag string) (string, error) {
	deviceID, err := h.createDevice(tag)
	if err != nil {
		return "", err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return "", err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return "", err
	}
	response, err := h.recordMeasurement(caseID, 26.5, "Rui")
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return caseID, nil
}

func (h *harness) readyCase(tag string) (string, error) {
	deviceID, err := h.createDevice(tag)
	if err != nil {
		return "", err
	}
	opened, err := h.openCase(deviceID)
	if err != nil {
		return "", err
	}
	caseID, err := nestedString(opened.Body, "case", "id")
	if err != nil {
		return "", err
	}
	response, err := h.recordMeasurement(caseID, 25.2, "Rui")
	if err != nil {
		return "", err
	}
	if err := expectStatus(response, http.StatusCreated); err != nil {
		return "", err
	}
	return caseID, nil
}

func expectStatus(response apiResponse, expected int) error {
	if response.Status != expected {
		return fmt.Errorf("status = %d, expected %d: %s", response.Status, expected, response.Raw)
	}
	return nil
}

func expectErrorCode(response apiResponse, expected string) error {
	code, err := nestedString(response.Body, "error", "code")
	if err != nil {
		return err
	}
	if code != expected {
		return fmt.Errorf("error code = %q, expected %q", code, expected)
	}
	return nil
}

func nestedString(root map[string]any, path ...string) (string, error) {
	value, err := nestedValue(root, path...)
	if err != nil {
		return "", err
	}
	text, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s is not a string", strings.Join(path, "."))
	}
	return text, nil
}

func nestedBool(root map[string]any, path ...any) (bool, error) {
	value, err := nestedAny(root, path...)
	if err != nil {
		return false, err
	}
	flag, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%v is not a boolean", path)
	}
	return flag, nil
}

func nestedFloat(root map[string]any, path ...string) (float64, error) {
	value, err := nestedValue(root, path...)
	if err != nil {
		return 0, err
	}
	number, ok := value.(float64)
	if !ok {
		return 0, fmt.Errorf("%s is not a number", strings.Join(path, "."))
	}
	return number, nil
}

func nestedSlice(root map[string]any, key string) ([]any, error) {
	value, ok := root[key]
	if !ok {
		return nil, fmt.Errorf("missing %s", key)
	}
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("%s is not an array", key)
	}
	return items, nil
}

func nestedSliceOrEmpty(root map[string]any, key string) []any {
	value, ok := root[key]
	if !ok || value == nil {
		return nil
	}
	items, _ := value.([]any)
	return items
}

func hasEvent(root map[string]any, kind string) bool {
	items, err := nestedSlice(root, "events")
	if err != nil {
		return false
	}
	for _, item := range items {
		event, ok := item.(map[string]any)
		if !ok {
			continue
		}
		if event["kind"] == kind {
			return true
		}
	}
	return false
}

func nestedValue(root map[string]any, path ...string) (any, error) {
	parts := make([]any, 0, len(path))
	for _, part := range path {
		parts = append(parts, part)
	}
	return nestedAny(root, parts...)
}

func nestedAny(root map[string]any, path ...any) (any, error) {
	var current any = root
	for _, part := range path {
		switch typed := part.(type) {
		case string:
			object, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s is not an object", typed)
			}
			value, ok := object[typed]
			if !ok {
				return nil, fmt.Errorf("missing %s", typed)
			}
			current = value
		case int:
			items, ok := current.([]any)
			if !ok {
				return nil, fmt.Errorf("index %d is not inside an array", typed)
			}
			if typed < 0 || typed >= len(items) {
				return nil, fmt.Errorf("index %d is out of range", typed)
			}
			current = items[typed]
		default:
			return nil, fmt.Errorf("unsupported path element %T", part)
		}
	}
	return current, nil
}
