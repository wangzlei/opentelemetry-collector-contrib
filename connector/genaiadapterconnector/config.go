package genaiadapterconnector

// Config defines configuration for the thirdpartyconnector.
type Config struct{}

// Validate checks the connector configuration for errors.
func (cfg *Config) Validate() error {
	return nil
}
