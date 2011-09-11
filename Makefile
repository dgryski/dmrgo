include $(GOROOT)/src/Make.inc

TARG=dmrgo
GOFILES=\
	runners.go\
	proto.go\

include $(GOROOT)/src/Make.pkg
